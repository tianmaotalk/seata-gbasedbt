/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.rm.datasource.sql.struct.cache;

import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.loader.LoadLevel;
import io.seata.common.util.StringUtils;
import io.seata.rm.datasource.sql.struct.ColumnMeta;
import io.seata.rm.datasource.sql.struct.IndexMeta;
import io.seata.rm.datasource.sql.struct.IndexType;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.sqlparser.util.JdbcConstants;

import java.sql.*;

/**
 * The type Table meta cache.
 *
 * @author tianmaotalk
 */
@LoadLevel(name = JdbcConstants.GBASEDBT)
public class GBasedbtTableMetaCache extends AbstractTableMetaCache {

    @Override
    protected String getCacheKey(Connection connection, String tableName, String resourceId) {
        StringBuilder cacheKey = new StringBuilder(resourceId);
        cacheKey.append(".");

        //separate it to catalog and tableName
        String[] tableNameWithSchema = tableName.split("\\.");
        String defaultTableName = tableNameWithSchema.length > 1 ? tableNameWithSchema[1] : tableNameWithSchema[0];

        //oracle does not implement supportsMixedCaseIdentifiers in DatabaseMetadata
        if (defaultTableName.contains("\"")) {
            cacheKey.append(defaultTableName.replace("\"", ""));
        } else {
            // oracle default store in upper case
            cacheKey.append(defaultTableName.toUpperCase());
        }

        return cacheKey.toString();
    }

    @Override
    protected TableMeta fetchSchema(Connection connection, String tableName) throws SQLException {
        try {
            //TODO get owner for table
            String owner = null;
            return resultSetMetaToSchema(connection, tableName, connection.getCatalog(), owner);
        } catch (SQLException sqlEx) {
            throw sqlEx;
        } catch (Exception e) {
            throw new SQLException(String.format("Failed to fetch schema of %s", tableName), e);
        }
    }

    private TableMeta resultSetMetaToSchema(Connection connection, String tableName, String catalog,String owner) throws SQLException {
        TableMeta tm = new TableMeta();
        tm.setTableName(tableName);
        DatabaseMetaData dbmd=connection.getMetaData();
        Statement stmt=connection.createStatement();
//        String[] schemaTable = tableName.split("\\.");
//        String catalog = schemaTable.length > 1 ? schemaTable[0] : dbmd.getUserName();
//        tableName = schemaTable.length > 1 ? schemaTable[1] : tableName;
        if (catalog.contains("\"")) {
            catalog = catalog.replace("\"", "");
        } else {
            catalog = catalog.toLowerCase();
        }

        if (tableName.contains("\"")) {
            tableName = tableName.replace("\"", "");

        } else {
            tableName = tableName.toLowerCase();
        }
        //无法获取到owner，所以目前不加owner条件
        String sql="select * from sysindexes i, sysconstraints const, systables t, syscolumns c where i.idxname=const.idxname and c.tabid=t.tabid and t.tabid=i.tabid and t.tabname='"+tableName+"' "
                +"and (i.part1=c.colno or i.part2=c.colno or i.part3=c.colno or i.part4=c.colno or i.part5=c.colno or i.part6=c.colno or i.part7=c.colno or i.part8=c.colno or i.part9=c.colno or i.part10=c.colno or i.part11=c.colno or i.part12=c.colno or i.part13=c.colno or i.part14=c.colno or i.part15=c.colno or i.part16=c.colno ) order by t.tabid";

        try (ResultSet rsColumns = dbmd.getColumns(catalog, owner, tableName, "%");
             ResultSet rsIndex = stmt.executeQuery(sql);
        ) {
            while (rsColumns.next()) {
                ColumnMeta col = new ColumnMeta();
                col.setTableCat(rsColumns.getString("TABLE_CAT"));
                col.setTableSchemaName(rsColumns.getString("TABLE_SCHEM"));
                col.setTableName(rsColumns.getString("TABLE_NAME"));
                col.setColumnName(rsColumns.getString("COLUMN_NAME"));
                col.setDataType(rsColumns.getInt("DATA_TYPE"));
                col.setDataTypeName(rsColumns.getString("TYPE_NAME"));
                col.setColumnSize(rsColumns.getInt("COLUMN_SIZE"));
                col.setDecimalDigits(rsColumns.getInt("DECIMAL_DIGITS"));
                col.setNumPrecRadix(rsColumns.getInt("NUM_PREC_RADIX"));
                col.setNullAble(rsColumns.getInt("NULLABLE"));
                col.setRemarks(rsColumns.getString("REMARKS"));
                col.setColumnDef(rsColumns.getString("COLUMN_DEF"));
                col.setSqlDataType(rsColumns.getInt("SQL_DATA_TYPE"));
                col.setSqlDatetimeSub(rsColumns.getInt("SQL_DATETIME_SUB"));
                col.setCharOctetLength(rsColumns.getInt("CHAR_OCTET_LENGTH"));
                col.setOrdinalPosition(rsColumns.getInt("ORDINAL_POSITION"));
                col.setIsNullAble(rsColumns.getString("IS_NULLABLE"));

                tm.getAllColumns().put(col.getColumnName(), col);
            }

            while (rsIndex.next()) {
                String indexName = rsIndex.getString("idxname");
                if (StringUtils.isNullOrEmpty(indexName)) {
                    continue;
                }
                String colName = rsIndex.getString("colname");
                ColumnMeta col = tm.getAllColumns().get(colName);
                if (tm.getAllIndexes().containsKey(indexName)) {
                    IndexMeta index = tm.getAllIndexes().get(indexName);
                    index.getValues().add(col);
                } else {
                    IndexMeta index = new IndexMeta();
                    index.setIndexName(indexName);
                    index.setNonUnique(rsIndex.getString("idxtype").equals("U") ? false : true);
                    index.setIndexQualifier(null);
                    index.setIndexName(rsIndex.getString("idxname"));
                    index.setType((short)3);
                    index.setOrdinalPosition(0);
                    index.setAscOrDesc(null);
                    index.setCardinality(0);
                    index.getValues().add(col);
                    if (rsIndex.getString("constrtype").equals("U")) {
                        index.setIndextype(IndexType.UNIQUE);
                    } else if (rsIndex.getString("constrtype").equals("P")) {
                        index.setIndextype(IndexType.PRIMARY);
                    } else {
                        index.setIndextype(IndexType.NORMAL);
                    }
                    tm.getAllIndexes().put(indexName, index);

                }
            }

            if (tm.getAllIndexes().isEmpty()) {
                throw new ShouldNeverHappenException(String.format("Could not found any index in the table: %s", tableName));
            }
        }

        return tm;
    }
}
