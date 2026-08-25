// Licensed to the Apache Software Foundation (ASF).

#include "exec/hdf5/hdfs-hdf5-scanner.h"

#include "common/status.h"
#include "exec/hdfs-scan-node-base.h"
#include "exec/hdfs-scan-node.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/types.h"
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
  // (path, iceberg schema JSON, iceberg filter JSON). The schema makes the
  // reader table-agnostic; the filter lets it materialise only matching rows.
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, scanner_cl_, "<init>",
      "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V", &ctor_));
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
  // ScannerContext has no filename(); HdfsScanner::Open sets stream_.
  if (stream_ == nullptr) {
    return Status("#SL.00000023.HDF5SCAN no scan stream");
  }
  jstring path = env->NewStringUTF(stream_->filename());
  RETURN_ERROR_IF_EXC(env);
  jstring schema_json = env->NewStringUTF(scan_node_->hdf5_schema_json().c_str());
  RETURN_ERROR_IF_EXC(env);
  jstring filter_json = env->NewStringUTF(scan_node_->hdf5_filter_json().c_str());
  RETURN_ERROR_IF_EXC(env);
  jobject local = env->NewObject(scanner_cl_, ctor_, path, schema_json, filter_json);
  env->DeleteLocalRef(path);
  env->DeleteLocalRef(schema_json);
  env->DeleteLocalRef(filter_json);
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
  while (!scan_node_->ReachedLimitShared() && !row_batch->AtCapacity()) {
    jobjectArray jrow =
        static_cast<jobjectArray>(env->CallObjectMethod(jscanner_, get_next_));
    RETURN_ERROR_IF_EXC(env);
    if (jrow == nullptr) {
      eos_ = true;
      return Status::OK();
    }
    int row_idx = row_batch->AddRow();
    TupleRow* tuple_row = row_batch->GetRow(row_idx);
    tuple_row->SetTuple(0, tuple);
    const int n = env->GetArrayLength(jrow);
    const auto& slots = tuple_desc->slots();
    jclass number_cl = env->FindClass("java/lang/Number");
    for (const SlotDescriptor* sd : slots) {
      if (sd->IsVirtual()) continue;
      const int idx = sd->col_pos();
      if (idx < 0 || idx >= n) {
        tuple->SetNull(sd->null_indicator_offset());
        continue;
      }
      jobject cell = env->GetObjectArrayElement(jrow, idx);
      if (cell == nullptr) {
        tuple->SetNull(sd->null_indicator_offset());
        continue;
      }
      void* slot = tuple->GetSlot(sd->tuple_offset());
      const PrimitiveType ptype = sd->type().type;
      if (env->IsInstanceOf(cell, number_cl)) {
        switch (ptype) {
          case TYPE_TINYINT:
            *reinterpret_cast<int8_t*>(slot) = static_cast<int8_t>(
                env->CallByteMethod(cell, env->GetMethodID(number_cl, "byteValue", "()B")));
            break;
          case TYPE_SMALLINT:
            *reinterpret_cast<int16_t*>(slot) = static_cast<int16_t>(
                env->CallShortMethod(cell, env->GetMethodID(number_cl, "shortValue", "()S")));
            break;
          case TYPE_INT:
            *reinterpret_cast<int32_t*>(slot) = env->CallIntMethod(
                cell, env->GetMethodID(number_cl, "intValue", "()I"));
            break;
          case TYPE_BIGINT:
            *reinterpret_cast<int64_t*>(slot) = env->CallLongMethod(
                cell, env->GetMethodID(number_cl, "longValue", "()J"));
            break;
          case TYPE_FLOAT:
            *reinterpret_cast<float*>(slot) = env->CallFloatMethod(
                cell, env->GetMethodID(number_cl, "floatValue", "()F"));
            break;
          case TYPE_DOUBLE:
            *reinterpret_cast<double*>(slot) = env->CallDoubleMethod(
                cell, env->GetMethodID(number_cl, "doubleValue", "()D"));
            break;
          case TYPE_DECIMAL: {
            // The Java side hands DECIMAL cells over as their unscaled integer
            // at the column's scale (see IcebergHdf5Scanner.GetNext), which is
            // exactly Impala's in-memory DECIMAL representation. Width follows
            // precision: 4 bytes to p=9, 8 to p=18, 16 to p=38.
            const int64_t unscaled = env->CallLongMethod(
                cell, env->GetMethodID(number_cl, "longValue", "()J"));
            switch (sd->type().GetByteSize()) {
              case 4:
                *reinterpret_cast<int32_t*>(slot) = static_cast<int32_t>(unscaled);
                break;
              case 8:
                *reinterpret_cast<int64_t*>(slot) = unscaled;
                break;
              case 16:
                *reinterpret_cast<__int128_t*>(slot) = unscaled;
                break;
              default:
                env->DeleteLocalRef(number_cl);
                env->DeleteLocalRef(cell);
                env->DeleteLocalRef(jrow);
                return Status("#SL.00000023.HDF5SCAN unsupported DECIMAL width");
            }
            break;
          }
          default:
            env->DeleteLocalRef(number_cl);
            env->DeleteLocalRef(cell);
            env->DeleteLocalRef(jrow);
            return Status("#SL.00000023.HDF5SCAN unsupported numeric slot type");
        }
      } else {
        env->DeleteLocalRef(number_cl);
        env->DeleteLocalRef(cell);
        env->DeleteLocalRef(jrow);
        return Status("#SL.00000023.HDF5SCAN non-numeric HDF5 cell");
      }
      env->DeleteLocalRef(cell);
    }
    env->DeleteLocalRef(number_cl);
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
  if (row_batch != nullptr) {
    row_batch->tuple_data_pool()->AcquireData(template_tuple_pool_.get(), false);
    if (scan_node_->HasRowBatchQueue()) {
      static_cast<HdfsScanNode*>(scan_node_)->AddMaterializedRowBatch(
          std::unique_ptr<RowBatch>(row_batch));
    }
  } else if (template_tuple_pool_ != nullptr) {
    template_tuple_pool_->FreeAll();
  }
  if (context_ != nullptr) context_->ReleaseCompletedResources(true);
  if (stream_ != nullptr) {
    scan_node_->RangeComplete(
        THdfsFileFormat::HDF5, stream_->file_desc()->file_compression);
  }
  CloseInternal();
}

}  // namespace impala
