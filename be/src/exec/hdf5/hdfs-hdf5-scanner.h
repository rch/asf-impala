// Licensed to the Apache Software Foundation (ASF).

#ifndef IMPALA_EXEC_HDFS_HDF5_SCANNER_H_
#define IMPALA_EXEC_HDFS_HDF5_SCANNER_H_

#include <jni.h>
#include "exec/hdfs-scanner.h"

namespace impala {

/// Iceberg HDF5 data files: JNI to IcebergHdf5Scanner (FormatModel + jhdf).
class HdfsHdf5Scanner : public HdfsScanner {
 public:
  HdfsHdf5Scanner(HdfsScanNodeBase* scan_node, RuntimeState* state);
  ~HdfsHdf5Scanner() override;

  static Status InitJNI();
  static Status IssueInitialRanges(
      HdfsScanNodeBase* scan_node, const std::vector<HdfsFileDesc*>& files);

  Status Open(ScannerContext* context) override;
  Status GetNextInternal(RowBatch* row_batch) override;
  void Close(RowBatch* row_batch) override;
  Status InitNewRange() override { return Status::OK(); }
  THdfsFileFormat::type file_format() const override {
    return THdfsFileFormat::HDF5;
  }

 private:
  static jclass scanner_cl_;
  static jmethodID ctor_;
  static jmethodID get_next_;
  static jmethodID close_;

  jobject jscanner_ = nullptr;
};

}  // namespace impala
#endif
