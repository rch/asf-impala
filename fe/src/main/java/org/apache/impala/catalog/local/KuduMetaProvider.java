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

package org.apache.impala.catalog.local;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Immutable;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.FileDescriptor;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.catalog.HdfsPartitionLocationCompressor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.SqlConstraints;
import org.apache.impala.catalog.VirtualColumn;
import org.apache.impala.compat.MetastoreShim;
import static org.apache.impala.analysis.Analyzer.ACCESSTYPE_READWRITE;
import org.apache.impala.catalog.local.LocalIcebergTable.TableParams;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialTableInfo;
import org.apache.impala.thrift.TValidWriteIdList;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaProvider implementation that uses a PostgreSQL catalog registry
 * to discover Kudu tables. Table metadata (schema, partitioning) is
 * derived from the Kudu master, not from HMS.
 *
 * The registry schema is:
 *   catalog_databases(name, description, location, owner, parameters)
 *   catalog_tables(db_name, table_name, table_type, parameters)
 *
 * For Kudu tables, parameters JSONB stores kudu.master_addresses and
 * kudu.table_name. The actual schema is always read from Kudu master
 * by LocalKuduTable.loadFromKudu().
 */
public class KuduMetaProvider implements MetaProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(KuduMetaProvider.class);

  private final String jdbcUrl_;
  private final String kuduMasterAddresses_;
  private final AuthorizationPolicy authPolicy_ = new AuthorizationPolicy();

  /**
   * @param properties must contain:
   *   - "jdbc.url" — JDBC URL for the catalog registry database
   *   - "kudu.master_addresses" — comma-separated Kudu master addresses
   */
  public KuduMetaProvider(Properties properties) {
    this.jdbcUrl_ = properties.getProperty("jdbc.url",
        "jdbc:postgresql://localhost:5455/signals_catalog");
    this.kuduMasterAddresses_ = properties.getProperty("kudu.master_addresses",
        "127.0.0.1:7051");
    LOG.info("KuduMetaProvider initialized: jdbc={}, kudu_masters={}",
        jdbcUrl_, kuduMasterAddresses_);
  }

  private Connection getConnection() throws SQLException {
    return DriverManager.getConnection(jdbcUrl_);
  }

  @Override
  public String getURI() {
    return "Kudu (" + kuduMasterAddresses_ + ")";
  }

  @Override
  public AuthorizationPolicy getAuthPolicy() {
    return authPolicy_;
  }

  @Override
  public boolean isReady() {
    return true;
  }

  @Override
  public void waitForIsReady(long timeoutMs) {
    // Direct provider is always ready.
  }

  @Override
  public ImmutableList<String> loadDbList() throws TException {
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "SELECT name FROM catalog_databases ORDER BY name");
         ResultSet rs = ps.executeQuery()) {
      ImmutableList.Builder<String> dbs = ImmutableList.builder();
      while (rs.next()) {
        dbs.add(rs.getString(1));
      }
      return dbs.build();
    } catch (SQLException e) {
      throw new TException("Failed to load database list from catalog registry", e);
    }
  }

  @Override
  public Database loadDb(String dbName) throws TException {
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "SELECT name, description, location, owner FROM catalog_databases " +
             "WHERE name = ?")) {
      ps.setString(1, dbName);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new NoSuchObjectException("Database not found: " + dbName);
        }
        Database db = new Database();
        db.setName(rs.getString("name"));
        db.setDescription(rs.getString("description"));
        db.setLocationUri(rs.getString("location"));
        db.setOwnerName(rs.getString("owner"));
        return db;
      }
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (SQLException e) {
      throw new TException("Failed to load database: " + dbName, e);
    }
  }

  @Override
  public ImmutableCollection<TBriefTableMeta> loadTableList(String dbName)
      throws TException {
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "SELECT table_name, table_type FROM catalog_tables " +
             "WHERE db_name = ? AND table_type IN ('KUDU', 'VIEW') " +
             "ORDER BY table_name")) {
      ps.setString(1, dbName);
      try (ResultSet rs = ps.executeQuery()) {
        ImmutableList.Builder<TBriefTableMeta> ret = ImmutableList.builder();
        while (rs.next()) {
          TBriefTableMeta meta = new TBriefTableMeta(rs.getString("table_name"));
          String typ = rs.getString("table_type");
          meta.setMsType("VIEW".equals(typ) ? "VIEW" : "TABLE");
          ret.add(meta);
        }
        return ret.build();
      }
    } catch (SQLException e) {
      throw new TException("Failed to load table list for db: " + dbName, e);
    }
  }

  @Override
  public Pair<Table, TableMetaRef> loadTable(String dbName, String tableName)
      throws TException {
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "SELECT table_type, parameters->>'view.original' AS view_original, " +
             "parameters->>'view.expanded' AS view_expanded " +
             "FROM catalog_tables WHERE db_name = ? AND table_name = ? " +
             "AND table_type IN ('KUDU', 'VIEW')")) {
      ps.setString(1, dbName);
      ps.setString(2, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new NoSuchObjectException(
              String.format("Table not found: %s.%s", dbName, tableName));
        }
        String tableType = rs.getString("table_type");
        Table msTable = new Table();
        msTable.setDbName(dbName);
        msTable.setTableName(tableName);

        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(Collections.emptyList());
        sd.setInputFormat("");
        sd.setOutputFormat("");
        sd.setSortCols(Collections.emptyList());
        SerDeInfo serde = new SerDeInfo();
        serde.setSerializationLib("");
        serde.setParameters(Collections.emptyMap());
        sd.setSerdeInfo(serde);
        sd.setLocation("");
        msTable.setSd(sd);
        msTable.setPartitionKeys(Collections.emptyList());

        if ("VIEW".equals(tableType)) {
          msTable.setTableType(TableType.VIRTUAL_VIEW.toString());
          String original = rs.getString("view_original");
          String expanded = rs.getString("view_expanded");
          if (expanded == null || expanded.isEmpty()) {
            throw new TException("VIEW " + dbName + "." + tableName
                + " has no view.expanded in catalog_tables");
          }
          msTable.setViewOriginalText(original != null ? original : expanded);
          msTable.setViewExpandedText(expanded);
          Map<String, String> params = new HashMap<>();
          msTable.setParameters(params);
        } else {
          // Kudu: LocalKuduTable.loadFromKudu() reads schema from the master.
          msTable.setTableType(TableType.MANAGED_TABLE.toString());
          Map<String, String> params = new HashMap<>();
          params.put(KuduTable.KEY_STORAGE_HANDLER, KuduTable.KUDU_STORAGE_HANDLER);
          params.put(KuduTable.KEY_MASTER_HOSTS, kuduMasterAddresses_);
          params.put(KuduTable.KEY_TABLE_NAME, "impala::" + dbName + "." + tableName);
          msTable.setParameters(params);
        }

        MetastoreShim.setTableAccessType(msTable, ACCESSTYPE_READWRITE);

        long loadingTime = System.currentTimeMillis();
        TableMetaRef ref = new KuduTableMetaRefImpl(dbName, tableName, msTable,
            loadingTime);
        return Pair.create(msTable, ref);
      }
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (SQLException e) {
      throw new TException(
          String.format("Failed to load table: %s.%s", dbName, tableName), e);
    }
  }

  @Override
  public Pair<Table, TableMetaRef> getTableIfPresent(String dbName, String tableName) {
    try {
      return loadTable(dbName, tableName);
    } catch (TException e) {
      LOG.debug("Table not present: {}.{}", dbName, tableName);
      return null;
    }
  }

  @Override
  public String loadNullPartitionKeyValue() throws MetaException, TException {
    return "__HIVE_DEFAULT_PARTITION__";
  }

  @Override
  public List<PartitionRef> loadPartitionList(TableMetaRef table)
      throws MetaException, TException {
    // Kudu tables are not HMS-partitioned.
    return ImmutableList.of(
        new KuduPartitionRefImpl(KuduPartitionRefImpl.UNPARTITIONED_NAME));
  }

  @Override
  public SqlConstraints loadConstraints(TableMetaRef table, Table msTbl)
      throws TException {
    return null;
  }

  @Override
  public Map<String, PartitionMetadata> loadPartitionsByRefs(
      TableMetaRef table, List<String> partitionColumnNames,
      ListMap<TNetworkAddress> hostIndex, List<PartitionRef> partitionRefs)
      throws CatalogException, TException {
    Map<String, PartitionMetadata> ret = new HashMap<>();
    ret.put("", new KuduPartitionMetadataImpl(((KuduTableMetaRefImpl)table).msTable_));
    return ret;
  }

  @Override
  public List<ColumnStatisticsObj> loadTableColumnStatistics(TableMetaRef table,
      List<String> colNames) throws TException {
    // Kudu has internal stats; return empty.
    return Collections.emptyList();
  }

  @Override
  public List<String> loadFunctionNames(String dbName) throws TException {
    throw new UnsupportedOperationException(
        "Functions not supported by KuduMetaProvider");
  }

  @Override
  public ImmutableList<Function> loadFunction(String dbName, String functionName)
      throws TException {
    throw new UnsupportedOperationException(
        "Functions not supported by KuduMetaProvider");
  }

  @Override
  public ImmutableList<DataSource> loadDataSources() throws TException {
    throw new UnsupportedOperationException(
        "DataSources not supported by KuduMetaProvider");
  }

  @Override
  public DataSource loadDataSource(String dsName) throws TException {
    throw new UnsupportedOperationException(
        "DataSources not supported by KuduMetaProvider");
  }

  @Override
  public TPartialTableInfo loadIcebergTable(final TableMetaRef table) throws TException {
    throw new UnsupportedOperationException(
        "Iceberg tables not supported by KuduMetaProvider");
  }

  @Override
  public org.apache.iceberg.Table loadIcebergApiTable(final TableMetaRef table,
      TableParams params, Table msTable) throws TException {
    throw new UnsupportedOperationException(
        "Iceberg tables not supported by KuduMetaProvider");
  }

  @Override
  public TValidWriteIdList getValidWriteIdList(TableMetaRef ref) {
    return null;
  }

  @Override
  public Iterable<HdfsCachePool> getHdfsCachePools() {
    return Collections.emptyList();
  }

  // ── DDL helpers for SignalsDdlExecutor ─────────────────────────────────

  /**
   * Register a new database in the catalog registry.
   */
  public void createDatabase(String name, String description, String location)
      throws SQLException {
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "INSERT INTO catalog_databases (name, description, location) " +
             "VALUES (?, ?, ?)")) {
      ps.setString(1, name);
      ps.setString(2, description);
      ps.setString(3, location);
      ps.executeUpdate();
    }
  }

  /**
   * Remove a database from the catalog registry.
   */
  public void dropDatabase(String name) throws SQLException {
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "DELETE FROM catalog_databases WHERE name = ?")) {
      ps.setString(1, name);
      ps.executeUpdate();
    }
  }

  /**
   * Register a new Kudu table in the catalog registry.
   */
  public void registerTable(String dbName, String tableName, String kuduTableName)
      throws SQLException {
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "INSERT INTO catalog_tables (db_name, table_name, table_type, parameters) " +
             "VALUES (?, ?, 'KUDU', ?::jsonb) " +
             "ON CONFLICT (db_name, table_name) DO UPDATE SET " +
             "table_type = EXCLUDED.table_type, parameters = EXCLUDED.parameters")) {
      ps.setString(1, dbName);
      ps.setString(2, tableName);
      String params = String.format(
          "{\"kudu.table_name\":\"%s\",\"kudu.master_addresses\":\"%s\"}",
          kuduTableName, kuduMasterAddresses_);
      ps.setString(3, params);
      ps.executeUpdate();
    }
  }

  /**
   * Register an Impala view in the catalog registry (HMS-free CREATE VIEW).
   * SQL text is stored as jsonb strings so JDBC handles quoting.
   */
  public void registerView(String dbName, String tableName,
      String originalSql, String expandedSql) throws SQLException {
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "INSERT INTO catalog_tables (db_name, table_name, table_type, parameters) "
                 + "VALUES (?, ?, 'VIEW', jsonb_build_object("
                 + "'view.original', ?::text, 'view.expanded', ?::text)) "
                 + "ON CONFLICT (db_name, table_name) DO UPDATE SET "
                 + "table_type = EXCLUDED.table_type, parameters = EXCLUDED.parameters")) {
      ps.setString(1, dbName);
      ps.setString(2, tableName);
      ps.setString(3, originalSql);
      ps.setString(4, expandedSql);
      ps.executeUpdate();
    }
  }

  /**
   * @return KUDU, ICEBERG, VIEW, or null if the name is not registered
   */
  public String getTableType(String dbName, String tableName) throws SQLException {
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "SELECT table_type FROM catalog_tables "
                 + "WHERE db_name = ? AND table_name = ?")) {
      ps.setString(1, dbName);
      ps.setString(2, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) return null;
        return rs.getString(1);
      }
    }
  }

  /**
   * Unregister a table from the catalog registry.
   */
  public void unregisterTable(String dbName, String tableName) throws SQLException {
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(
             "DELETE FROM catalog_tables WHERE db_name = ? AND table_name = ?")) {
      ps.setString(1, dbName);
      ps.setString(2, tableName);
      ps.executeUpdate();
    }
  }

  // ── Inner classes ─────────────────────────────────────────────────────

  @Immutable
  private static class KuduPartitionRefImpl implements PartitionRef {
    private static final String UNPARTITIONED_NAME = "";
    private final String name_;

    public KuduPartitionRefImpl(String name) {
      this.name_ = name;
    }

    @Override
    public String getName() {
      return name_;
    }
  }

  private static class KuduPartitionMetadataImpl implements PartitionMetadata {
    private final Table msTable_;

    public KuduPartitionMetadataImpl(Table msTable) {
      this.msTable_ = msTable;
    }

    @Override
    public Map<String, String> getHmsParameters() { return Collections.emptyMap(); }

    @Override
    public long getWriteId() { return -1; }

    @Override
    public HdfsStorageDescriptor getInputFormatDescriptor() {
      String tblName = msTable_.getDbName() + "." + msTable_.getTableName();
      try {
        return HdfsStorageDescriptor.fromStorageDescriptor(tblName, msTable_.getSd());
      } catch (HdfsStorageDescriptor.InvalidStorageDescriptorException e) {
        throw new LocalCatalogException(String.format(
            "Invalid input format descriptor for table %s", tblName), e);
      }
    }

    @Override
    public HdfsPartitionLocationCompressor.Location getLocation() {
      return new HdfsPartitionLocationCompressor(0).new Location(
          msTable_.getSd().getLocation());
    }

    @Override
    public ImmutableList<FileDescriptor> getFileDescriptors() {
      return ImmutableList.of();
    }

    @Override
    public ImmutableList<FileDescriptor> getInsertFileDescriptors() {
      return ImmutableList.of();
    }

    @Override
    public ImmutableList<FileDescriptor> getDeleteFileDescriptors() {
      return ImmutableList.of();
    }

    @Override
    public boolean hasIncrementalStats() { return false; }

    @Override
    public byte[] getPartitionStats() { return null; }

    @Override
    public boolean isMarkedCached() { return false; }

    @Override
    public long getLastCompactionId() {
      throw new UnsupportedOperationException(
          "Compaction id is not provided with KuduMetaProvider implementation");
    }
  }

  private static class KuduTableMetaRefImpl implements TableMetaRef {
    private final String dbName_;
    private final String tableName_;
    private final Table msTable_;
    private final long loadingTimeMs_;

    public KuduTableMetaRefImpl(String dbName, String tableName, Table msTable,
                                long loadingTimeMs) {
      this.dbName_ = dbName;
      this.tableName_ = tableName;
      this.msTable_ = msTable;
      this.loadingTimeMs_ = loadingTimeMs;
    }

    @Override
    public boolean isPartitioned() { return false; }

    @Override
    public boolean isMarkedCached() { return false; }

    @Override
    public List<String> getPartitionPrefixes() {
      return Collections.emptyList();
    }

    @Override
    public boolean isTransactional() { return false; }

    @Override
    public List<VirtualColumn> getVirtualColumns() {
      return Collections.emptyList();
    }

    @Override
    public long getCatalogVersion() { return 0; }

    @Override
    public long getLoadedTimeMs() { return loadingTimeMs_; }
  }
}
