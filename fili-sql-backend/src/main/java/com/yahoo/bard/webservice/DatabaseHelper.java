// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;

/**
 * Created by hinterlong on 6/8/17.
 */
public class DatabaseHelper {
    private DatabaseHelper() {
    }

    public static String getDateTimeColumn(Connection connection, String tableName) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM " + tableName + " LIMIT 1");
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData rsmd = resultSet.getMetaData();
        // todo as of now I've only tested this with timestamp
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            int jdbcType = rsmd.getColumnType(i);
            if (jdbcType == JDBCType.DATE.getVendorTypeNumber()) {
                return rsmd.getColumnName(i);
            }
        }

        if (resultSet.next()) {
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                String s = resultSet.getString(i);
                String columnName = rsmd.getColumnName(i).toLowerCase();
                if (columnName.contains("date") || columnName.contains("time")) {
                    try {
                        Timestamp time = Timestamp.valueOf(s);
                        return rsmd.getColumnName(i);
                    } catch (IllegalArgumentException ignored) {
                    }
                }
            }
        }
        throw new IllegalStateException("No DateTime column was found");
    }

    /**
     * Converts a {@link ResultSet} to string.
     * From CalciteAssert
     * */
    public static class ResultSetFormatter {
        final StringBuilder sb = new StringBuilder();

        public ResultSetFormatter resultSet(ResultSet resultSet) throws SQLException {
            return resultSet(resultSet, -1);
        }

        // result set cannot be reset after rows have been read, this consumes results by reading them
        public ResultSetFormatter resultSet(ResultSet resultSet, int max)
                throws SQLException {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = 0;
            while (resultSet.next() && (count < max || max < 0)) {
                rowToString(resultSet, metaData);
                sb.append("\n");
                count++;
            }
            return this;
        }

        /** Converts one row to a string. */
        ResultSetFormatter rowToString(
                ResultSet resultSet,
                ResultSetMetaData metaData
        ) throws SQLException {
            int n = metaData.getColumnCount();
            if (n > 0) {
                for (int i = 1; ; i++) {
                    sb.append(metaData.getColumnLabel(i))
                            .append("=")
                            .append(adjustValue(resultSet.getString(i)));
                    if (i == n) {
                        break;
                    }
                    sb.append("; ");
                }
            }
            return this;
        }

        protected String adjustValue(String string) {
            return string;
        }

        public Collection<String> toStringList(
                ResultSet resultSet,
                Collection<String> list
        ) throws SQLException {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                rowToString(resultSet, metaData);
                list.add(sb.toString());
                sb.setLength(0);
            }
            return list;
        }

        /** Flushes the buffer and returns its previous contents. */
        public String string() {
            String s = sb.toString();
            sb.setLength(0);
            return s;
        }
    }
}
