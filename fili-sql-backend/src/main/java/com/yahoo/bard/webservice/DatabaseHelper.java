package com.yahoo.bard.webservice;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

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
}
