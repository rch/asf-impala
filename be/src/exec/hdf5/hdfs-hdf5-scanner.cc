// Licensed to the Apache Software Foundation (ASF).

#include "exec/hdf5/hdfs-hdf5-scanner.h"

#include "common/status.h"
#include "exec/hdfs-scan-node-base.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "util/jni-util.h"

using namespace std;

namespace impala {

jclass HdfsHdf5Scanner::scanner_cl_ = nullptr;
jmethodID HdfsHdf5Scanner::ctor_ = nullptr;
jmethodID HdfsHdf5Scanner::get_next_ = nullptr;
jmethodID HdfsHdf5Scanner::close_ = nullptr;

HdfsHdf5Scanner::HdfsHdf5Scanner(HdfsScanNodeBase* scan_node, RuntimeState* state)
    : HdfsScanner(scan_node, state) {}

HdfsHdf5Scanner::~HdfsHdf5Scanner() {}

Status HdfsHdf5Scanner::InitJNI() {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(
      env, "org/apache/impala/util/IcebergHdf5Scanner", &scanner_cl_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(
      env, scanner_cl_, "<init>", "(Ljava/lang/String;)V", &ctor_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(
      env, scanner_cl_, "GetNext", "()[Ljava/lang/Object;", &get_next_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, scanner_cl_, "Close", "()V", &close_));
  return Status::OK();
}

Status HdfsHdf5Scanner::IssueInitialRanges(
    HdfsScanNodeBase* scan_node, const vector<HdfsFileDesc*>& files) {
  for (HdfsFileDesc* file : files) {
    RETURN_IF_ERROR(scan_node->AddDiskIoRanges(file, EnqueueLocation::TAIL));
  }
  return Status::OK();
}

Status HdfsHdf5Scanner::Open(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsScanner::Open(context));
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("#SL.00000023.HDF5SCAN no JVM");
  jstring path = env->NewStringUTF(context_->filename().c_str());
  RETURN_ERROR_IF_EXC(env);
  jobject local = env->NewObject(scanner_cl_, ctor_, path);
  env->DeleteLocalRef(path);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, local, &jscanner_));
  env->DeleteLocalRef(local);
  return Status::OK();
}

Status HdfsHdf5Scanner::GetNextInternal(RowBatch* row_batch) {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("#SL.00000023.HDF5SCAN no JVM");
  const TupleDescriptor* tuple_desc = scan_node_->tuple_desc();
  uint8_t* tuple_buffer;
  int64_t tuple_buffer_size;
  RETURN_IF_ERROR(row_batch->ResizeAndAllocateTupleBuffer(
      state_, &tuple_buffer_size, &tuple_buffer));
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);
  tuple->Init(tuple_buffer_size);
  while (!scan_node_->ReachedLimit() && !row_batch->AtCapacity()) {
    int row_idx = row_batch->AddRow();
    TupleRow* tuple_row = row_batch->GetRow(row_idx);
    tuple_row->SetTuple(0, tuple);
    jobjectArray jrow =
        static_cast<jobjectArray>(env->CallObjectMethod(jscanner_, get_next_));
    RETURN_ERROR_IF_EXC(env);
    if (jrow == nullptr) {
      eos_ = true;
      return Status::OK();
    }
    const int n = env->GetArrayLength(jrow);
    const auto& slots = tuple_desc->slots();
    for (int i = 0; i < n && i < static_cast<int>(slots.size()); ++i) {
      jobject cell = env->GetObjectArrayElement(jrow, i);
      if (cell == nullptr) {
        tuple->SetNull(slots[i]->null_indicator_offset());
        continue;
      }
      jclass icl = env->FindClass("java/lang/Number");
      if (env->IsInstanceOf(cell, icl)) {
        jmethodID iv = env->GetMethodID(icl, "intValue", "()I");
        int32_t v = env->CallIntMethod(cell, iv);
        *reinterpret_cast<int32_t*>(tuple->GetSlot(slots[i]->tuple_offset())) = v;
      }
      env->DeleteLocalRef(icl);
      env->DeleteLocalRef(cell);
    }
    env->DeleteLocalRef(jrow);
    row_batch->CommitLastRow();
    tuple = reinterpret_cast<Tuple*>(
        reinterpret_cast<uint8_t*>(tuple) + tuple_desc->byte_size());
  }
  return Status::OK();
}

void HdfsHdf5Scanner::Close(RowBatch* row_batch) {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env != nullptr && jscanner_ != nullptr) {
    env->CallVoidMethod(jscanner_, close_);
    env->DeleteGlobalRef(jscanner_);
    jscanner_ = nullptr;
  }
  HdfsScanner::Close(row_batch);
}

}  // namespace impala
