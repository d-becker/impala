// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.catalog;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketches;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.util.Pair;

import org.apache.impala.common.FileSystemUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PuffinStatsLoader {
  private static final Logger LOG = LoggerFactory.getLogger(PuffinStatsLoader.class);

  public static class PuffinStatsRecord {
    public final StatisticsFile file;
    public final BlobMetadata blob;
    public final long ndv;

    public PuffinStatsRecord(StatisticsFile file, BlobMetadata blob, long ndv) {
      this.file = file;
      this.blob = blob;
      this.ndv = ndv;
    }

    // TODO: No need for this method.
    @Override
    public String toString() {
      return "{file: " + file + ", blob: " + blob + ", ndv: " + ndv + "}";
    }
  }

  public static Map<Integer, PuffinStatsRecord> loadPuffinStats(IcebergTable iceTable) {
    Map<Integer, PuffinStatsRecord> result = new HashMap<>();
    org.apache.iceberg.Table icebergApiTable = iceTable.getIcebergApiTable();
    final List<StatisticsFile> statsFiles = icebergApiTable.statisticsFiles();
    for (StatisticsFile statsFile : statsFiles) {
      loadStatsFromFile(statsFile, iceTable, result);
    }
    return result;
  }

  private static void loadStatsFromFile(StatisticsFile statsFile, IcebergTable iceTable,
      Map<Integer, PuffinStatsRecord> result) {
    final long currentSnapshotId = iceTable.snapshotId();
    if (statsFile.snapshotId() != currentSnapshotId) return;

    // Keep track of the Iceberg column field ids for which we read statistics from this
    // Puffin file. If we run into an error reading the contents of the file, the file may
    // be corrupt so we want to remove values already read from it from the overall
    // result.
    List<Integer> fieldIdsFromFile = new ArrayList<>();
    try (PuffinReader puffinReader = createPuffinReader(statsFile)) {
      // TODO.
      // PuffinReader puffinReader = createPuffinReader(statsFile);
      List<BlobMetadata> blobs = getBlobs(puffinReader, currentSnapshotId);

      // The 'UncheckedIOException' can be thrown from the 'next()' method of the
      // iterator. Statistics that are loaded successfully before an exception is thrown
      // are discarded because the file is probably corrupt.
      for (Pair<BlobMetadata, ByteBuffer> puffinData: puffinReader.readAll(blobs)) {
        BlobMetadata blobMetadata = puffinData.first();
        ByteBuffer blobData = puffinData.second();

        loadStatsFromBlob(blobMetadata, blobData, iceTable, statsFile,
            result, fieldIdsFromFile);
      }
    } catch (NotFoundException e) {
      // 'result' has not been touched yet.
      logWarning(iceTable.getFullName(), statsFile.path(), true, e);
    } catch (Exception e) {
      // We restore 'result' to the previous state because the Puffin file may be corrupt.
      logWarning(iceTable.getFullName(), statsFile.path(), false, e);
      result.keySet().removeAll(fieldIdsFromFile);
    }
  }

  private static void logWarning(String tableName, String statsFilePath,
      boolean fileMissing, Exception e) {
    String missingStr = fileMissing ? "missing " : "";
    LOG.warn(String.format("Could not load Iceberg Puffin column statistics "
        + "for table '%s' from %sPuffin file '%s'. Exception: %s",
        tableName, missingStr, statsFilePath, e));
  }

  private static PuffinReader createPuffinReader(StatisticsFile statsFile) {
    org.apache.iceberg.io.InputFile puffinFile = HadoopInputFile.fromLocation(
        statsFile.path(), FileSystemUtil.getConfiguration());

    // return Puffin.read(puffinFile)
    //     .withFileSize(statsFile.fileSizeInBytes())
    //     .withFooterSize(statsFile.fileFooterSizeInBytes())
    //     .build();

    // TODO: no need for logging?
    PuffinReader res = Puffin.read(puffinFile)
        .withFileSize(statsFile.fileSizeInBytes())
        .withFooterSize(statsFile.fileFooterSizeInBytes())
        .build();
    LOG.info("Created Puffin reader for file " + statsFile.path() + ".");
    return res;
  }

  private static List<BlobMetadata> getBlobs(PuffinReader puffinReader,
      long currentSnapshotId) throws java.io.IOException {
    FileMetadata fileMetadata = puffinReader.fileMetadata();
    return fileMetadata.blobs().stream()
      .filter(blob ->
          blob.snapshotId() == currentSnapshotId &&
          blob.type().equals("apache-datasketches-theta-v1") &&
          blob.inputFields().size() == 1)
      .collect(Collectors.toList());
  }

  private static void loadStatsFromBlob(BlobMetadata blobMetadata, ByteBuffer blobData,
      IcebergTable iceTable, StatisticsFile statsFile,
      Map<Integer, PuffinStatsRecord> result, List<Integer> fieldIdsFromFile) {
    Preconditions.checkState(blobMetadata.inputFields().size() == 1);
    int fieldId = blobMetadata.inputFields().get(0);

    // Memory.wrap(ByteBuffer) would result in an incorrect deserialisation.
    double ndv = Sketches.getEstimate(Memory.wrap(getBytes(blobData)));
    long ndvRounded = Math.round(ndv);
    PuffinStatsRecord record = new PuffinStatsRecord(statsFile, blobMetadata, ndvRounded);

    PuffinStatsRecord prevRecord = result.putIfAbsent(fieldId, record);
    if (prevRecord == null) {
      fieldIdsFromFile.add(fieldId);
    } else {
      IcebergColumn col = iceTable.getColumnByIcebergFieldId(fieldId);
      Preconditions.checkNotNull(col);
      LOG.warn(String.format("Multiple NDV values from Puffin statistics for column '%s' "
          + "of table '%s'. Old value (from file %s): %s; new value (from file %s): %s. "
          + "Using the old value.", col.getName(), iceTable.getFullName(),
          prevRecord.file.path(), prevRecord.ndv, record.file.path(), record.ndv));
    }
  }

  // Gets the bytes from the provided 'ByteBuffer' without advancing buffer position. The
  // returned byte array may be shared with the buffer.
  private static byte[] getBytes(ByteBuffer byteBuffer) {
    if (byteBuffer.hasArray() && byteBuffer.arrayOffset() == 0 &&
        byteBuffer.position() == 0) {
      byte[] array = byteBuffer.array();
      if (byteBuffer.remaining() == array.length) {
        return array;
      }
    }

    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.asReadOnlyBuffer().get(bytes);
    return bytes;
  }
}
