// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.

package org.apache.impala.util;

import java.nio.file.Path;
import java.util.Iterator;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
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
        Types.NestedField.required(1, "gpu_index", Types.IntegerType.get()),
        Types.NestedField.required(2, "sample", Types.IntegerType.get()),
        Types.NestedField.required(3, "value", Types.IntegerType.get()));
    iterable_ = Hdf5GenericFormatModel.create()
        .readBuilder(Files.localInput(Path.of(path).toFile()))
        .project(schema)
        .build();
    rows_ = iterable_.iterator();
  }

  /** JNI: next row as boxed ints, or null at EOF. */
  public Object[] GetNext() {
    if (rows_ == null || !rows_.hasNext()) {
      return null;
    }
    Record r = rows_.next();
    return new Object[] {r.get(0), r.get(1), r.get(2)};
  }

  public void Close() {
    try {
      if (iterable_ != null) iterable_.close();
    } catch (Exception e) {
      throw new IllegalStateException(GURU + " " + e.getMessage(), e);
    }
  }
}
