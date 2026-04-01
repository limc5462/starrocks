// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.jdbc;

import com.google.common.collect.ImmutableSet;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ClickhouseSchemaResolver extends JDBCSchemaResolver {
    Map<String, String> properties;

    public static final Set<String> SUPPORTED_TABLE_TYPES = new HashSet<>(
            Arrays.asList("LOG TABLE", "MEMORY TABLE", "TEMPORARY TABLE", "VIEW", "DICTIONARY", "SYSTEM TABLE",
                    "REMOTE TABLE", "TABLE"));

    public ClickhouseSchemaResolver(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Collection<String> listSchemas(Connection connection) {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (!schemaName.equalsIgnoreCase("INFORMATION_SCHEMA") && !schemaName.equalsIgnoreCase("system")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }
    }


    @Override
    public ResultSet getTables(Connection connection, String dbName) throws SQLException {
        String tableTypes = properties.get("table_types");
        if (null != tableTypes) {
            String[] tableTypesArray = tableTypes.split(",");
            if (tableTypesArray.length == 0) {
                throw new StarRocksConnectorException("table_types should be populated with table types separated by " +
                        "comma, e.g. 'TABLE,VIEW'. Currently supported type includes:" +
                        String.join(",", SUPPORTED_TABLE_TYPES));
            }

            for (String tt : tableTypesArray) {
                if (!SUPPORTED_TABLE_TYPES.contains(tt)) {
                    throw new StarRocksConnectorException("Unsupported table type found: " + tt,
                            ",Currently supported table types includes:" + String.join(",", SUPPORTED_TABLE_TYPES));
                }
            }
            return connection.getMetaData().getTables(connection.getCatalog(), dbName, null, tableTypesArray);
        }
        return connection.getMetaData().getTables(connection.getCatalog(), dbName, null,
                SUPPORTED_TABLE_TYPES.toArray(new String[SUPPORTED_TABLE_TYPES.size()]));

    }

    @Override
    public ResultSet getColumns(Connection connection, String dbName, String tblName) throws SQLException {
        return connection.getMetaData().getColumns(connection.getCatalog(), dbName, tblName, "%");
    }


    @Override
    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) {
        PrimitiveType primitiveType;
        switch (dataType) {
            case Types.TINYINT:
                primitiveType = PrimitiveType.TINYINT;
                break;
            case Types.SMALLINT:
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case Types.INTEGER:
                primitiveType = PrimitiveType.INT;
                break;
            case Types.BIGINT:
                primitiveType = PrimitiveType.BIGINT;
                break;
            case Types.NUMERIC:
                primitiveType = PrimitiveType.LARGEINT;
                break;
            case Types.FLOAT:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case Types.DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case Types.BOOLEAN:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case Types.VARCHAR:
                return TypeFactory.createVarcharType(65533);
            case Types.DATE:
                primitiveType = PrimitiveType.DATE;
                break;
            case Types.TIMESTAMP:
                primitiveType = PrimitiveType.DATETIME;
                break;
            case Types.DECIMAL:
                // Decimal(9,9), first 9 is precision, second 9 is scale
                if (typeName.startsWith("Nullable")) {
                    typeName = typeName.replace("Nullable", "");
                }
                String[] precisionAndScale =
                        typeName.replace("Decimal", "").replace("(", "")
                                .replace(")", "").replace(" ", "")
                                .split(",");
                if (precisionAndScale.length != 2) {
                    // should not go here, but if it does, we make it DECIMALV2.
                    throw new StarRocksConnectorException(
                            "Cannot extract precision and scale from Decimal typename:" + typeName);
                } else {
                    int precision = Integer.parseInt(precisionAndScale[0]);
                    int scale = Integer.parseInt(precisionAndScale[1]);
                    return TypeFactory.createUnifiedDecimalType(precision, scale);
                }
            case Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE:
                return TypeFactory.createVarcharType(65533);
            case Types.OTHER:
                return convertAggregateType(typeName);
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }
        return TypeFactory.createType(primitiveType);
    }

    /**
     * Converts a ClickHouse aggregate column type (AggregateFunction / SimpleAggregateFunction)
     * reported as {@link Types#OTHER} by the JDBC driver into a StarRocks {@link Type}.
     *
     * <p>For {@code AggregateFunction(func, innerType)}, the column stores ClickHouse's internal
     * binary intermediate aggregation state, which is opaque bytes when read directly via JDBC.
     * It is therefore mapped to VARCHAR as a safe, lossless fallback.
     *
     * <p>For {@code SimpleAggregateFunction(func, innerType)}, the stored value is always of
     * {@code innerType} regardless of the function name, so we resolve innerType directly.
     */
    private Type convertAggregateType(String typeName) {
        if (typeName.startsWith("AggregateFunction(")) {
            // AggregateFunction columns contain binary intermediate aggregation state.
            // Reading them directly via JDBC yields opaque bytes; map to VARCHAR as a safe fallback.
            return TypeFactory.createVarcharType(65533);
        } else if (typeName.startsWith("SimpleAggregateFunction(")) {
            // SimpleAggregateFunction always stores the actual value in the declared argument type;
            // the function name does not affect what is stored.
            String inner = typeName.substring("SimpleAggregateFunction(".length(), typeName.length() - 1);
            int splitIdx = findFirstTopLevelComma(inner);
            if (splitIdx < 0) {
                return TypeFactory.createType(PrimitiveType.UNKNOWN_TYPE);
            }
            String innerTypeName = inner.substring(splitIdx + 1).trim();
            return resolveInnerType(innerTypeName);
        }
        return TypeFactory.createType(PrimitiveType.UNKNOWN_TYPE);
    }



    /**
     * Finds the index of the first comma at bracket nesting depth 0 in {@code s}.
     * This correctly handles nested types such as {@code Decimal(9,2)} or
     * function names such as {@code quantile(0.5)}.
     *
     * @return index of the comma, or {@code -1} if not found.
     */
    private int findFirstTopLevelComma(String s) {
        int depth = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Resolves a raw ClickHouse type name string (possibly wrapped in {@code Nullable(…)})
     * directly to a StarRocks {@link Type}.
     *
     * <p>Supported base types and their mappings:
     * <pre>
     *   Int8                            → TINYINT
     *   UInt8, Int16                    → SMALLINT
     *   UInt16, Int32                   → INT
     *   Int64, UInt32                   → BIGINT
     *   UInt64, Int128, UInt128,
     *   Int256, UInt256                 → LARGEINT
     *   Float32                         → FLOAT
     *   Float64                         → DOUBLE
     *   Bool                            → BOOLEAN
     *   String, FixedString(N)          → VARCHAR(65533)
     *   Date, Date32                    → DATE
     *   DateTime, DateTime(tz)          → DATETIME
     *   DateTime64(p), DateTime64(p,tz) → DATETIME
     *   Decimal(P, S)                   → DecimalV3(P, S)
     * </pre>
     */
    private Type resolveInnerType(String innerTypeName) {
        // Strip Nullable wrapper if present.
        String baseTypeName = innerTypeName;
        if (innerTypeName.startsWith("Nullable(") && innerTypeName.endsWith(")")) {
            baseTypeName = innerTypeName.substring("Nullable(".length(), innerTypeName.length() - 1).trim();
        }

        switch (baseTypeName) {
            case "Int8":
                return TypeFactory.createType(PrimitiveType.TINYINT);
            case "UInt8":
            case "Int16":
                return TypeFactory.createType(PrimitiveType.SMALLINT);
            case "UInt16":
            case "Int32":
                return TypeFactory.createType(PrimitiveType.INT);
            case "Int64":
            case "UInt32":
                return TypeFactory.createType(PrimitiveType.BIGINT);
            case "UInt64":
            case "Int128":
            case "UInt128":
            case "Int256":
            case "UInt256":
                return TypeFactory.createType(PrimitiveType.LARGEINT);
            case "Float32":
                return TypeFactory.createType(PrimitiveType.FLOAT);
            case "Float64":
                return TypeFactory.createType(PrimitiveType.DOUBLE);
            case "Bool":
                return TypeFactory.createType(PrimitiveType.BOOLEAN);
            case "String":
                return TypeFactory.createVarcharType(65533);
            case "Date":
            case "Date32":
                return TypeFactory.createType(PrimitiveType.DATE);
            default:
                break;
        }

        // Prefix-matched types.
        if (baseTypeName.startsWith("FixedString(")) {
            return TypeFactory.createVarcharType(65533);
        }
        // DateTime with optional timezone: DateTime('Asia/Shanghai')
        if (baseTypeName.equals("DateTime") || baseTypeName.startsWith("DateTime(")) {
            return TypeFactory.createType(PrimitiveType.DATETIME);
        }
        // DateTime64 with precision and optional timezone: DateTime64(3) or DateTime64(3, 'Asia/Shanghai')
        if (baseTypeName.startsWith("DateTime64(")) {
            return TypeFactory.createType(PrimitiveType.DATETIME);
        }
        // Decimal(P, S)
        if (baseTypeName.startsWith("Decimal(") && baseTypeName.endsWith(")")) {
            String decimalInner = baseTypeName.substring("Decimal(".length(), baseTypeName.length() - 1);
            String[] parts = decimalInner.split(",");
            if (parts.length == 2) {
                try {
                    int precision = Integer.parseInt(parts[0].trim());
                    int scale = Integer.parseInt(parts[1].trim());
                    return TypeFactory.createUnifiedDecimalType(precision, scale);
                } catch (NumberFormatException ignored) {
                    // fall through to UNKNOWN_TYPE
                }
            }
        }

        return TypeFactory.createType(PrimitiveType.UNKNOWN_TYPE);
    }

}
