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

// TODO: Find a better name and put into its own file?
// TODO: Document
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
