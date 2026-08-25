// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.

package org.apache.impala.util;

import java.io.File;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.zndx.semantics.iceberg.Hdf5FormatModels;
import org.zndx.semantics.iceberg.Hdf5GenericFormatModel;

/**
 * HS2 / JNI entry for Iceberg HDF5 data files (Percy-style hierarchical
 * storage: Kudu hot, Iceberg+HDF5 on object store). Uses File Format API
 * {@link Hdf5GenericFormatModel}, not the C++ Parquet scanner.
 *
 * <p>The BE constructs one instance per data file with the table's Iceberg
 * schema and the planner's pushed predicates, both as Iceberg JSON
 * ({@code SchemaParser} / {@code ExpressionParser}). Rows come back in table
 * column order so {@code SlotDescriptor.col_pos()} indexes them directly.
 */
public final class IcebergHdf5Scanner {
  public static final String GURU = "#SL.00000023.HDF5SCAN";

  static {
    Hdf5FormatModels.register();
  }

  private final Iterator<Record> rows_;
  private final boolean[] isDecimal_;
  private CloseableIterable<Record> iterable_;

  /** Legacy entry: gpu_metrics_tier1 grain, no predicate. */
  public IcebergHdf5Scanner(String path) {
    this(path, SchemaParser.toJson(legacyGpuMetricsSchema()), "");
  }

  public IcebergHdf5Scanner(String path, String schemaJson, String filterJson) {
    if (schemaJson == null || schemaJson.isEmpty()) {
      throw new IllegalArgumentException(GURU + " empty Iceberg schema for " + path);
    }
    Schema schema = SchemaParser.fromJson(schemaJson);
    ReadBuilder<Record, Schema> rb =
        Hdf5GenericFormatModel.create().readBuilder(inputFile(path)).project(schema);
    if (filterJson != null && !filterJson.isEmpty()) {
      Expression filter = ExpressionParser.fromJson(filterJson, schema);
      rb = rb.filter(filter);
    }
    iterable_ = rb.build();
    rows_ = iterable_.iterator();
    List<Types.NestedField> cols = schema.columns();
    isDecimal_ = new boolean[cols.size()];
    for (int i = 0; i < cols.size(); i++) {
      isDecimal_[i] = cols.get(i).type().typeId() == Type.TypeID.DECIMAL;
    }
  }

  static Schema legacyGpuMetricsSchema() {
    return new Schema(
        Types.NestedField.required(1, "epoch_hour", Types.IntegerType.get()),
        Types.NestedField.required(2, "ts_ns", Types.LongType.get()),
        Types.NestedField.required(3, "gpu_index", Types.IntegerType.get()),
        Types.NestedField.required(4, "power_w", Types.FloatType.get()),
        Types.NestedField.required(5, "util_pct", Types.FloatType.get()),
        Types.NestedField.required(6, "mem_used_mb", Types.FloatType.get()),
        Types.NestedField.required(7, "temp_c", Types.FloatType.get()));
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

  /**
   * JNI: next row as boxed numbers, or null at EOF.
   *
   * <p>DECIMAL cells cross as their unscaled {@code Long} at the column scale:
   * that is Impala's in-memory DECIMAL representation, and it avoids a
   * BigDecimal→double→int round trip that would reintroduce the IEEE artifacts
   * the tier was designed to exclude.
   */
  public Object[] GetNext() {
    if (rows_ == null || !rows_.hasNext()) {
      return null;
    }
    Record r = rows_.next();
    int n = r.size();
    Object[] out = new Object[n];
    for (int i = 0; i < n; i++) {
      Object v = r.get(i);
      if (v != null && i < isDecimal_.length && isDecimal_[i] && v instanceof BigDecimal) {
        v = ((BigDecimal) v).unscaledValue().longValueExact();
      }
      out[i] = v;
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
