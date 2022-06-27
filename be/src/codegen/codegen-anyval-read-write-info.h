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

#pragma once

#include <vector>

namespace llvm {
class BasicBlock;
class Value;
}

namespace impala {

struct ColumnType;
class LlvmBuilder;
class LlvmCodeGen;

/// This struct is used in conversions to and from 'CodegenAnyVal', i.e. 'AnyVal' objects
/// in codegen code. This struct is an interface between sources and destinations: sources
/// generate an instance of this struct and destinations take that instance and use it to
/// write the value.
///
/// The other side can for example be tuples from which we read (in the
/// case of 'SlotRef'), tuples we write into (in case of materialisation, see
/// Tuple::CodegenMaterializeExprs()) but other cases exist, too. The main advantage is
/// that sources do not have to know how to write their destinations, only how to read the
/// values (and vice versa). This also makes it possible, should there be need for it, to
/// leave out 'CodegenAnyVal' and convert directly between a source and a destination that
/// know how to read and write 'CodegenAnyValReadWriteInfo's.
///
/// An instance of 'CodegenAnyValReadWriteInfo' represents a value but also contains
/// information about how it is read and written in LLVM IR.
///
/// A source (for example 'SlotRef') should generate IR that starts in 'entry_block' (so
/// that other IR code can branch to it), perform NULL checking and branch to 'null_block'
/// and 'non_null_block' accordingly. The source is responsible for creating these blocks.
/// It is allowed to create more blocks, but these blocks should not be missing.
///
/// A destination should be able to rely on this structure, i.e. it should be able to
/// branch to 'entry_block' and to generate code in 'null_block' and 'non_null_block' to
/// write the value. It is also allowed to generate additional blocks but it should not
/// write into 'entry_block' or assume that the source only used the above mentioned
/// blocks.
///
/// Structs are represented recursively. The fields 'codegen', 'builder' and 'type' should
/// be filled by the source so that the destination can use them to generate IR code.
/// Other fields, such as 'fn_ctx_idx' and 'eval' may be needed in some cases but not in
/// others.
struct CodegenAnyValReadWriteInfo {
  LlvmCodeGen* codegen = nullptr;
  LlvmBuilder* builder = nullptr;
  const ColumnType* type = nullptr;

  int fn_ctx_idx = -1;
  llvm::Value* eval = nullptr; // Pointer to the ScalarExprEvaluator in LLVM code.

  // Possible parts the resulting value may be composed of.
  // Simple native types
  llvm::Value* val = nullptr;

  // String and collection types
  llvm::Value* ptr = nullptr;
  llvm::Value* len = nullptr;

  // Timestamp
  llvm::Value* time_of_day = nullptr;
  llvm::Value* date = nullptr;

  llvm::BasicBlock* entry_block = nullptr;

  // The block we branch to if the read value is null.
  llvm::BasicBlock* null_block = nullptr;

  // The block we branch to if the read value is not null.
  llvm::BasicBlock* non_null_block = nullptr;

  // Vector of 'CodegenAnyValReadWriteInfo's for children in case this one refers to a
  // struct.
  std::vector<CodegenAnyValReadWriteInfo> children;
};

} // namespace impala
