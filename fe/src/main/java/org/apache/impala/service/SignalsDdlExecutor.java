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

import java.sql.SQLException;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.util.EventSequence;
import org.apache.impala.catalog.local.KuduMetaProvider;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCreateDbParams;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDropDbParams;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes DDL operations without HMS. In HMS-free mode, DDL operations
 * are routed directly to the underlying storage engines:
 *
 * - Kudu tables: KuduCatalogOpExecutor creates the table in Kudu,
 *   then the table is registered in the PostgreSQL catalog registry.
 *
 * - Iceberg tables: IcebergCatalogOpExecutor creates the table via
 *   the Polaris REST catalog. No registry entry needed since Polaris
 *   is the source of truth.
 *
 * - Databases: Created/dropped in the PostgreSQL catalog registry.
 */
public class SignalsDdlExecutor {
  private static final Logger LOG =
      LoggerFactory.getLogger(SignalsDdlExecutor.class);

  private final KuduMetaProvider kuduMetaProvider_;

  public SignalsDdlExecutor(KuduMetaProvider kuduMetaProvider) {
    this.kuduMetaProvider_ = kuduMetaProvider;
  }

  /**
   * Create a database in the catalog registry.
   */
  public void createDatabase(TCreateDbParams params) throws ImpalaException {
    String dbName = params.getDb();
    String comment = params.getComment();
    String location = params.getLocation();
    if (location == null || location.isEmpty()) {
      location = "file:///tmp/signals-warehouse/" + dbName;
    }
    try {
      kuduMetaProvider_.createDatabase(dbName, comment, location);
      LOG.info("Created database '{}' in catalog registry", dbName);
    } catch (SQLException e) {
      if (params.if_not_exists && e.getMessage().contains("duplicate key")) {
        LOG.info("Database '{}' already exists (IF NOT EXISTS specified)", dbName);
        return;
      }
      throw new ImpalaRuntimeException(
          "Failed to create database in catalog registry: " + dbName, e);
    }
  }

  /**
   * Drop a database from the catalog registry.
   */
  public void dropDatabase(TDropDbParams params) throws ImpalaException {
    String dbName = params.getDb();
    try {
      kuduMetaProvider_.dropDatabase(dbName);
      LOG.info("Dropped database '{}' from catalog registry", dbName);
    } catch (SQLException e) {
      if (params.if_exists) {
        LOG.info("Database '{}' not found (IF EXISTS specified)", dbName);
        return;
      }
      throw new ImpalaRuntimeException(
          "Failed to drop database from catalog registry: " + dbName, e);
    }
  }

  /**
   * Create a table. Routes to Kudu or Iceberg based on table type.
   */
  public void createTable(TCreateTableParams params, Table msTbl,
      EventSequence catalogTimeline) throws ImpalaException {
    String dbName = msTbl.getDbName();
    String tableName = msTbl.getTableName();

    if (KuduTable.isKuduTable(msTbl)) {
      // Create the table in Kudu via KuduCatalogOpExecutor
      KuduCatalogOpExecutor.createSynchronizedTable(catalogTimeline, msTbl, params);
      LOG.info("Created Kudu table '{}.{}'", dbName, tableName);

      // Register in catalog registry
      String kuduTableName = msTbl.getParameters().get(KuduTable.KEY_TABLE_NAME);
      if (kuduTableName == null) {
        kuduTableName = "impala::" + dbName + "." + tableName;
      }
      try {
        kuduMetaProvider_.registerTable(dbName, tableName, kuduTableName);
        LOG.info("Registered Kudu table '{}.{}' in catalog registry", dbName, tableName);
      } catch (SQLException e) {
        // Rollback: drop the Kudu table if registry insert fails
        try {
          KuduCatalogOpExecutor.dropTable(msTbl, false, 0, catalogTimeline);
        } catch (Exception rollbackEx) {
          LOG.error("Failed to rollback Kudu table creation for {}.{}", dbName,
              tableName, rollbackEx);
        }
        throw new ImpalaRuntimeException(
            "Failed to register Kudu table in catalog registry", e);
      }
    } else if (IcebergTable.isIcebergTable(msTbl)) {
      // Iceberg tables are created via Polaris REST catalog.
      // IcebergCatalogOpExecutor handles this directly.
      LOG.info("Iceberg table '{}.{}' — delegating to IcebergCatalogOpExecutor",
          dbName, tableName);
      // The Iceberg table creation is handled by the standard code path
      // via IcebergCatalogOpExecutor.createTable(), which now delegates
      // to IcebergRESTCatalog.createTable() (implemented in Phase 2).
    } else {
      throw new ImpalaRuntimeException(
          "HMS-free mode only supports KUDU and ICEBERG table types. " +
          "Got: " + msTbl.getTableType());
    }
  }

  /**
   * Drop a table. Drops from Kudu (physical table) and unregisters from catalog registry.
   */
  public void dropTable(TDropTableOrViewParams params,
      EventSequence catalogTimeline) throws ImpalaException {
    String dbName = params.getTable_name().getDb_name();
    String tableName = params.getTable_name().getTable_name();
    boolean ifExists = params.if_exists;

    // Drop the physical table from Kudu
    String kuduMasters = System.getProperty("signals.kudu.master_addresses",
        "127.0.0.1:7051");
    String kuduTableName = "impala::" + dbName + "." + tableName;

    // Construct minimal msTbl for KuduCatalogOpExecutor.dropTable()
    Table msTbl = new Table();
    msTbl.setDbName(dbName);
    msTbl.setTableName(tableName);
    msTbl.setTableType("MANAGED_TABLE");
    java.util.Map<String, String> tblParams = new java.util.HashMap<>();
    tblParams.put(KuduTable.KEY_TABLE_NAME, kuduTableName);
    tblParams.put(KuduTable.KEY_MASTER_HOSTS, kuduMasters);
    tblParams.put(KuduTable.KEY_STORAGE_HANDLER, KuduTable.KUDU_STORAGE_HANDLER);
    msTbl.setParameters(tblParams);

    try {
      KuduCatalogOpExecutor.dropTable(msTbl, ifExists, 0, catalogTimeline);
      LOG.info("Dropped Kudu table '{}.{}' (kudu='{}')", dbName, tableName,
          kuduTableName);
    } catch (Exception e) {
      if (!ifExists) {
        throw new ImpalaRuntimeException(
            "Failed to drop Kudu table: " + kuduTableName, e);
      }
      LOG.debug("Kudu table '{}' not found (IF EXISTS specified)", kuduTableName);
    }

    // Unregister from catalog registry
    try {
      kuduMetaProvider_.unregisterTable(dbName, tableName);
      LOG.info("Unregistered table '{}.{}' from catalog registry", dbName, tableName);
    } catch (SQLException e) {
      if (!ifExists) {
        throw new ImpalaRuntimeException(
            "Failed to unregister table from catalog registry", e);
      }
      LOG.debug("Table '{}.{}' not found in catalog registry (IF EXISTS specified)",
          dbName, tableName);
    }
  }
}
