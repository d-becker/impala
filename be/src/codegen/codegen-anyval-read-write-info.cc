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

#include "codegen/codegen-anyval-read-write-info.h"

#include "codegen/llvm-codegen.h"

namespace impala {

void NonWritableBasicBlock::BranchTo(LlvmBuilder* builder) const {
  DCHECK(builder != nullptr);
  builder->CreateBr(basic_block_);
}

void NonWritableBasicBlock::CondBranchToAsTrue(LlvmBuilder* builder,
    llvm::Value* condition, const NonWritableBasicBlock& false_block) const {
  DCHECK(builder != nullptr);
  builder->CreateCondBr(condition, basic_block_, false_block.basic_block_);
}

void NonWritableBasicBlock::CondBranchToAsFalse(LlvmBuilder* builder,
    llvm::Value* condition, const NonWritableBasicBlock& true_block) const {
  DCHECK(builder != nullptr);
  builder->CreateCondBr(condition, true_block.basic_block_, basic_block_);
}

llvm::Value* CodegenAnyValReadWriteInfo::GetSimpleVal() const {
  DCHECK(val_ != nullptr);
  return val_;
}

const CodegenAnyValReadWriteInfo::PtrLenStruct& CodegenAnyValReadWriteInfo::GetPtrAndLen()
    const {
  DCHECK(ptr_len_struct_.ptr != nullptr);
  DCHECK(ptr_len_struct_.len != nullptr);
  return ptr_len_struct_;
}

const CodegenAnyValReadWriteInfo::TimestampStruct&
    CodegenAnyValReadWriteInfo::GetTimeAndDate() const {
  DCHECK(timestamp_struct_.time_of_day != nullptr);
  DCHECK(timestamp_struct_.date != nullptr);
  return timestamp_struct_;
}

void CodegenAnyValReadWriteInfo::SetSimpleVal(llvm::Value* val) {
  DCHECK(ptr_len_struct_.ptr == nullptr);
  DCHECK(ptr_len_struct_.len == nullptr);
  DCHECK(timestamp_struct_.time_of_day == nullptr);
  DCHECK(timestamp_struct_.date == nullptr);

  DCHECK(val != nullptr);

  val_ = val;
}

void CodegenAnyValReadWriteInfo::SetPtrAndLen(llvm::Value* ptr, llvm::Value* len) {
  DCHECK(val_ == nullptr);
  DCHECK(timestamp_struct_.time_of_day == nullptr);
  DCHECK(timestamp_struct_.date == nullptr);

  DCHECK(ptr != nullptr);
  DCHECK(len != nullptr);

  ptr_len_struct_.ptr = ptr;
  ptr_len_struct_.len = len;
}

void CodegenAnyValReadWriteInfo::SetTimeAndDate(llvm::Value* time_of_day,
    llvm::Value* date) {
  DCHECK(val_ == nullptr);
  DCHECK(ptr_len_struct_.ptr == nullptr);
  DCHECK(ptr_len_struct_.len == nullptr);

  DCHECK(time_of_day != nullptr);
  DCHECK(date != nullptr);

  timestamp_struct_.time_of_day = time_of_day;
  timestamp_struct_.date = date;
}

void CodegenAnyValReadWriteInfo::SetEval(llvm::Value* eval) {
  DCHECK(eval != nullptr);
  eval_ = eval;
}

void CodegenAnyValReadWriteInfo::SetFnCtxIdx(int fn_ctx_idx) {
  DCHECK_GE(fn_ctx_idx, -1);
  fn_ctx_idx_ = fn_ctx_idx;
}

void CodegenAnyValReadWriteInfo::SetBlocks(llvm::BasicBlock* entry_block,
    llvm::BasicBlock* null_block, llvm::BasicBlock* non_null_block) {
  DCHECK(entry_block != nullptr);
  DCHECK(null_block != nullptr);
  DCHECK(non_null_block != nullptr);

  entry_block_ = entry_block;
  null_block_ = null_block;
  non_null_block_ = non_null_block;
}

} // namespace impala
