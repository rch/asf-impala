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

package org.apache.impala.service;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.SQLException;

import org.apache.impala.catalog.local.KuduMetaProvider;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCreateDbParams;
import org.apache.impala.thrift.TDropDbParams;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.thrift.TTableName;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for SignalsDdlExecutor DDL routing logic.
 * Uses Mockito to mock KuduMetaProvider JDBC operations.
 */
public class SignalsDdlExecutorTest {

  private KuduMetaProvider mockProvider_;
  private SignalsDdlExecutor executor_;

  @Before
  public void setUp() {
    mockProvider_ = mock(KuduMetaProvider.class);
    executor_ = new SignalsDdlExecutor(mockProvider_);
  }

  // ── createDatabase ─────────────────────────────────────────────────

  @Test
  public void testCreateDatabase() throws Exception {
    doNothing().when(mockProvider_).createDatabase(
        anyString(), anyString(), anyString());

    TCreateDbParams params = new TCreateDbParams();
    params.setDb("my_db");
    params.setComment("My database");
    params.setOwner("admin");

    executor_.createDatabase(params);

    verify(mockProvider_).createDatabase(
        "my_db", "My database", "file:///tmp/signals-warehouse/my_db");
  }

  @Test
  public void testCreateDatabaseWithLocation() throws Exception {
    doNothing().when(mockProvider_).createDatabase(
        anyString(), anyString(), anyString());

    TCreateDbParams params = new TCreateDbParams();
    params.setDb("my_db");
    params.setComment("My database");
    params.setLocation("hdfs:///custom/location");
    params.setOwner("admin");

    executor_.createDatabase(params);

    verify(mockProvider_).createDatabase(
        "my_db", "My database", "hdfs:///custom/location");
  }

  @Test
  public void testCreateDatabaseIfNotExistsDuplicate() throws Exception {
    doThrow(new SQLException("duplicate key value violates unique constraint"))
        .when(mockProvider_).createDatabase(anyString(), anyString(), anyString());

    TCreateDbParams params = new TCreateDbParams();
    params.setDb("existing_db");
    params.setOwner("admin");
    params.setIf_not_exists(true);

    // Should not throw when if_not_exists is true
    executor_.createDatabase(params);
  }

  @Test
  public void testCreateDatabaseDuplicateWithoutIfNotExists() throws Exception {
    doThrow(new SQLException("duplicate key value violates unique constraint"))
        .when(mockProvider_).createDatabase(anyString(), anyString(), anyString());

    TCreateDbParams params = new TCreateDbParams();
    params.setDb("existing_db");
    params.setOwner("admin");
    // if_not_exists defaults to false

    try {
      executor_.createDatabase(params);
      fail("Expected ImpalaRuntimeException");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage().contains("Failed to create database"));
    }
  }

  // ── dropDatabase ───────────────────────────────────────────────────

  @Test
  public void testDropDatabase() throws Exception {
    doNothing().when(mockProvider_).dropDatabase(anyString());

    TDropDbParams params = new TDropDbParams();
    params.setDb("my_db");
    params.setIf_exists(false);
    params.setCascade(false);

    executor_.dropDatabase(params);

    verify(mockProvider_).dropDatabase("my_db");
  }

  @Test
  public void testDropDatabaseIfExistsOnFailure() throws Exception {
    doThrow(new SQLException("error")).when(mockProvider_).dropDatabase(anyString());

    TDropDbParams params = new TDropDbParams();
    params.setDb("nonexistent");
    params.setIf_exists(true);
    params.setCascade(false);

    // Should not throw when if_exists is true
    executor_.dropDatabase(params);
  }

  @Test
  public void testDropDatabaseFailsWithoutIfExists() throws Exception {
    doThrow(new SQLException("error")).when(mockProvider_).dropDatabase(anyString());

    TDropDbParams params = new TDropDbParams();
    params.setDb("bad_db");
    params.setIf_exists(false);
    params.setCascade(false);

    try {
      executor_.dropDatabase(params);
      fail("Expected ImpalaRuntimeException");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage().contains("Failed to drop database"));
    }
  }

  // ── dropTable ──────────────────────────────────────────────────────

  @Test
  public void testDropTable() throws Exception {
    doNothing().when(mockProvider_).unregisterTable(anyString(), anyString());

    TDropTableOrViewParams params = new TDropTableOrViewParams();
    params.setTable_name(new TTableName("test_db", "my_table"));
    params.setIf_exists(false);
    params.setPurge(false);

    executor_.dropTable(params, null);

    verify(mockProvider_).unregisterTable("test_db", "my_table");
  }

  @Test
  public void testDropTableIfExistsOnFailure() throws Exception {
    doThrow(new SQLException("not found"))
        .when(mockProvider_).unregisterTable(anyString(), anyString());

    TDropTableOrViewParams params = new TDropTableOrViewParams();
    params.setTable_name(new TTableName("test_db", "nonexistent"));
    params.setIf_exists(true);
    params.setPurge(false);

    // Should not throw when if_exists is true
    executor_.dropTable(params, null);
  }

  @Test
  public void testDropTableFailsWithoutIfExists() throws Exception {
    doThrow(new SQLException("connection error"))
        .when(mockProvider_).unregisterTable(anyString(), anyString());

    TDropTableOrViewParams params = new TDropTableOrViewParams();
    params.setTable_name(new TTableName("test_db", "bad_table"));
    params.setIf_exists(false);
    params.setPurge(false);

    try {
      executor_.dropTable(params, null);
      fail("Expected ImpalaRuntimeException");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage().contains("Failed to unregister table"));
    }
  }
}
