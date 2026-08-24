// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.

package org.apache.impala.util;

import java.io.File;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.zndx.semantics.iceberg.Hdf5FormatModels;
import org.zndx.semantics.iceberg.Hdf5GenericFormatModel;

/**
 * HS2 / JNI entry for Iceberg HDF5 data files (Percy-style hierarchical
 * storage: Kudu hot, Iceberg+HDF5 on object store). Uses File Format API
 * {@link Hdf5GenericFormatModel}, not the C++ Parquet scanner.
 */
public final class IcebergHdf5Scanner {
  public static final String GURU = "#SL.00000023.HDF5SCAN";

  static {
    Hdf5FormatModels.register();
  }

  private final Iterator<Record> rows_;
  private CloseableIterable<Record> iterable_;

  public IcebergHdf5Scanner(String path) {
    Schema schema = new Schema(
        Types.NestedField.required(1, "epoch_hour", Types.IntegerType.get()),
        Types.NestedField.required(2, "ts_ns", Types.LongType.get()),
        Types.NestedField.required(3, "gpu_index", Types.IntegerType.get()),
        Types.NestedField.required(4, "power_w", Types.FloatType.get()),
        Types.NestedField.required(5, "util_pct", Types.FloatType.get()),
        Types.NestedField.required(6, "mem_used_mb", Types.FloatType.get()),
        Types.NestedField.required(7, "temp_c", Types.FloatType.get()));
    iterable_ = Hdf5GenericFormatModel.create()
        .readBuilder(inputFile(path))
        .project(schema)
        .build();
    rows_ = iterable_.iterator();
  }

  static InputFile inputFile(String path) {
    if (path == null || path.isEmpty()) {
      throw new IllegalArgumentException(GURU + " empty HDF5 path");
    }
    if (path.startsWith("/") || path.startsWith("file:")) {
      String local = path.startsWith("file:") ? path.substring("file:".length()) : path;
      return Files.localInput(new File(local));
    }
    return HadoopInputFile.fromLocation(path, new Configuration());
  }

  /** JNI: next row as boxed ints, or null at EOF. */
  public Object[] GetNext() {
    if (rows_ == null || !rows_.hasNext()) {
      return null;
    }
    Record r = rows_.next();
    int n = r.size();
    Object[] out = new Object[n];
    for (int i = 0; i < n; i++) {
      out[i] = r.get(i);
    }
    return out;
  }

  public void Close() {
    try {
      if (iterable_ != null) iterable_.close();
    } catch (Exception e) {
      throw new IllegalStateException(GURU + " " + e.getMessage(), e);
    }
  }
}
