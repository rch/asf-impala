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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for KuduMetaProvider using an in-memory Derby database
 * to simulate the PostgreSQL catalog registry.
 *
 * Tests for loadTable() and registerTable() require PostgreSQL (they use
 * ::text and ::jsonb casts). Those are covered by integration tests.
 */
public class KuduMetaProviderTest {

  private static final String DERBY_URL =
      "jdbc:derby:memory:kudu_meta_test;create=true";

  private KuduMetaProvider provider_;
  private Connection setupConn_;

  @Before
  public void setUp() throws Exception {
    setupConn_ = DriverManager.getConnection(DERBY_URL);
    try (Statement s = setupConn_.createStatement()) {
      s.executeUpdate(
          "CREATE TABLE catalog_databases (" +
          "  name VARCHAR(255) PRIMARY KEY," +
          "  description VARCHAR(1024)," +
          "  location VARCHAR(1024)," +
          "  owner VARCHAR(255)," +
          "  parameters VARCHAR(4096)" +
          ")");
      s.executeUpdate(
          "CREATE TABLE catalog_tables (" +
          "  db_name VARCHAR(255)," +
          "  table_name VARCHAR(255)," +
          "  table_type VARCHAR(64)," +
          "  parameters VARCHAR(4096)," +
          "  PRIMARY KEY (db_name, table_name)" +
          ")");
    }

    // Seed test data
    try (PreparedStatement ps = setupConn_.prepareStatement(
        "INSERT INTO catalog_databases (name, description, location, owner) " +
        "VALUES (?, ?, ?, ?)")) {
      ps.setString(1, "test_db");
      ps.setString(2, "Test database");
      ps.setString(3, "file:///tmp/signals-warehouse/test_db");
      ps.setString(4, "admin");
      ps.executeUpdate();
    }

    try (PreparedStatement ps = setupConn_.prepareStatement(
        "INSERT INTO catalog_tables (db_name, table_name, table_type, parameters) " +
        "VALUES (?, ?, ?, ?)")) {
      ps.setString(1, "test_db");
      ps.setString(2, "kudu_t");
      ps.setString(3, "KUDU");
      ps.setString(4, "{\"kudu.table_name\":\"impala::test_db.kudu_t\"}");
      ps.executeUpdate();
    }

    Properties props = new Properties();
    props.setProperty("jdbc.url", DERBY_URL);
    props.setProperty("kudu.master_addresses", "127.0.0.1:7051");
    provider_ = new KuduMetaProvider(props);
  }

  @After
  public void tearDown() throws Exception {
    if (setupConn_ != null && !setupConn_.isClosed()) {
      try (Statement s = setupConn_.createStatement()) {
        s.executeUpdate("DROP TABLE catalog_tables");
        s.executeUpdate("DROP TABLE catalog_databases");
      }
      setupConn_.close();
    }
    try {
      DriverManager.getConnection("jdbc:derby:memory:kudu_meta_test;drop=true");
    } catch (SQLException e) {
      // Derby throws exception on successful drop — expected
    }
  }

  // ── Database operations (standard SQL) ─────────────────────────────

  @Test
  public void testLoadDbList() throws TException {
    ImmutableList<String> dbs = provider_.loadDbList();
    assertNotNull(dbs);
    assertEquals(1, dbs.size());
    assertEquals("test_db", dbs.get(0));
  }

  @Test
  public void testLoadDb() throws TException {
    Database db = provider_.loadDb("test_db");
    assertNotNull(db);
    assertEquals("test_db", db.getName());
    assertEquals("Test database", db.getDescription());
    assertEquals("file:///tmp/signals-warehouse/test_db", db.getLocationUri());
    assertEquals("admin", db.getOwnerName());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testLoadDbNotFound() throws TException {
    provider_.loadDb("nonexistent");
  }

  @Test
  public void testCreateAndDropDatabase() throws Exception {
    provider_.createDatabase("new_db", "New database",
        "file:///tmp/signals-warehouse/new_db");

    ImmutableList<String> dbs = provider_.loadDbList();
    assertTrue(dbs.contains("new_db"));

    Database db = provider_.loadDb("new_db");
    assertEquals("New database", db.getDescription());

    provider_.dropDatabase("new_db");

    dbs = provider_.loadDbList();
    assertTrue(!dbs.contains("new_db"));
  }

  // ── Table list operations (standard SQL) ───────────────────────────

  @Test
  public void testLoadTableList() throws TException {
    List<TBriefTableMeta> tables =
        (List<TBriefTableMeta>) provider_.loadTableList("test_db");
    assertNotNull(tables);
    assertEquals(1, tables.size());
    assertEquals("kudu_t", tables.get(0).getName());
  }

  @Test
  public void testLoadTableListEmptyDb() throws TException {
    List<TBriefTableMeta> tables =
        (List<TBriefTableMeta>) provider_.loadTableList("nonexistent");
    assertNotNull(tables);
    assertTrue(tables.isEmpty());
  }

  @Test
  public void testUnregisterTable() throws Exception {
    provider_.unregisterTable("test_db", "kudu_t");

    List<TBriefTableMeta> tables =
        (List<TBriefTableMeta>) provider_.loadTableList("test_db");
    assertTrue(tables.isEmpty());
  }

  // ── Provider metadata ──────────────────────────────────────────────

  @Test
  public void testGetURI() {
    assertEquals("Kudu (127.0.0.1:7051)", provider_.getURI());
  }

  @Test
  public void testIsReady() {
    assertTrue(provider_.isReady());
  }

  @Test
  public void testNullPartitionKeyValue() throws Exception {
    assertEquals("__HIVE_DEFAULT_PARTITION__",
        provider_.loadNullPartitionKeyValue());
  }

  @Test
  public void testGetTableIfPresentReturnsNull() {
    // getTableIfPresent calls loadTable which uses PG-specific ::text cast.
    // On Derby this will fail gracefully and return null.
    assertNull(provider_.getTableIfPresent("test_db", "nonexistent"));
  }

  @Test
  public void testLoadTableColumnStatisticsReturnsEmpty() throws Exception {
    // Column stats don't require a real TableMetaRef for the empty-list path
    assertTrue(provider_.loadTableColumnStatistics(null,
        ImmutableList.of("col1")).isEmpty());
  }

  // ── Unsupported operations ─────────────────────────────────────────

  @Test(expected = UnsupportedOperationException.class)
  public void testLoadFunctionNamesThrows() throws TException {
    provider_.loadFunctionNames("test_db");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testLoadDataSourcesThrows() throws TException {
    provider_.loadDataSources();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testLoadDataSourceThrows() throws TException {
    provider_.loadDataSource("test_source");
  }

  @Test
  public void testGetHdfsCachePoolsEmpty() {
    Iterable<org.apache.impala.catalog.HdfsCachePool> pools =
        provider_.getHdfsCachePools();
    assertNotNull(pools);
    assertTrue(!pools.iterator().hasNext());
  }

  @Test
  public void testGetValidWriteIdListReturnsNull() {
    assertNull(provider_.getValidWriteIdList(null));
  }
}
