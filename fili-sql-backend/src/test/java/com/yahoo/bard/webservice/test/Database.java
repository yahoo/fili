// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.test;

import static com.yahoo.bard.webservice.SQLConverter.THE_SCHEMA;

import com.yahoo.bard.webservice.TimeUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.sql.DataSource;

public class Database {
    private static final String DATABASE_URL = "jdbc:h2:mem:test";
    private static final String WIKITICKER_JSON_DATA = "wikiticker-2015-09-12-sampled.json";
    private static Connection connection;

    public static Connection getDatabase() throws SQLException, IOException {
        if (connection == null) {
            connection = DriverManager.getConnection(DATABASE_URL);
        } else {
            return connection;
        }

        List<WikitickerEntry> entries = readJsonFile();

        connection.createStatement().execute("CREATE SCHEMA " + THE_SCHEMA + ";" + " SET SCHEMA " + THE_SCHEMA);
        Statement s = connection.createStatement();
        s.execute("CREATE TABLE WIKITICKER (ID INT PRIMARY KEY," +
                "COMMENT VARCHAR(256)," +
                "COUNTRY_ISO_CODE VARCHAR(256)," +
                "ADDED INTEGER," +
                "TIME TIMESTAMP, " +
                "IS_NEW BOOLEAN," +
                "IS_ROBOT BOOLEAN," +
                "DELETED INTEGER," +
                "METRO_CODE VARCHAR(256)," +
                "IS_UNPATROLLED BOOLEAN," +
                "NAMESPACE VARCHAR(256)," +
                "PAGE VARCHAR(256)," +
                "COUNTRY_NAME VARCHAR(256)," +
                "CITY_NAME VARCHAR(256)," +
                "IS_MINOR BOOLEAN," +
                "USER VARCHAR(256)," +
                "DELTA INTEGER," +
                "IS_ANONYMOUS BOOLEAN," +
                "REGION_ISO_CODE VARCHAR(256)," +
                "CHANNEL VARCHAR(256)," +
                "REGION_NAME VARCHAR(256)," +
                ")"
        );

        String sqlInsert = "INSERT INTO WIKITICKER (" +
                "ID, COMMENT, COUNTRY_ISO_CODE, ADDED, TIME, IS_NEW, IS_ROBOT," +
                " DELETED, METRO_CODE, IS_UNPATROLLED, NAMESPACE, PAGE, COUNTRY_NAME," +
                " CITY_NAME, IS_MINOR, USER, DELTA, IS_ANONYMOUS, REGION_ISO_CODE, " +
                "CHANNEL, REGION_NAME" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        int i = 0;
        for (WikitickerEntry e : entries) {

            PreparedStatement preparedStatement = connection.prepareStatement(sqlInsert);
            preparedStatement.setInt(1, i);
            preparedStatement.setString(2, e.getComment());
            preparedStatement.setString(3, e.getCountryIsoCode());
            preparedStatement.setInt(4, e.getAdded());
            preparedStatement.setTimestamp(
                    5,
                    TimeUtils.timestampFromString(e.getTime())
            );
            preparedStatement.setBoolean(6, e.getIsNew());
            preparedStatement.setBoolean(7, e.getIsRobot());
            preparedStatement.setInt(8, e.getDeleted());
            preparedStatement.setString(9, e.getMetroCode());
            preparedStatement.setBoolean(10, e.getIsUnpatrolled());
            preparedStatement.setString(11, e.getNamespace());
            preparedStatement.setString(12, e.getPage());
            preparedStatement.setString(13, e.getCountryName());
            preparedStatement.setString(14, e.getCityName());
            preparedStatement.setBoolean(15, e.getIsMinor());
            preparedStatement.setString(16, e.getUser());
            preparedStatement.setInt(17, e.getDelta());
            preparedStatement.setBoolean(18, e.getIsAnonymous());
            preparedStatement.setString(19, e.getRegionIsoCode());
            preparedStatement.setString(20, e.getChannel());
            preparedStatement.setString(21, e.getRegionName());
            i++;
            preparedStatement.execute();
        }

        s.close();
        return connection;
    }

    public static List<WikitickerEntry> readJsonFile() throws IOException {
        ClassLoader classLoader = Database.class.getClassLoader();
        File file = new File(classLoader.getResource(WIKITICKER_JSON_DATA).getFile());

        String json = FileUtils.readFileToString(file);
        List<WikitickerEntry> entries = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        for (String line : json.split(System.lineSeparator())) {
            WikitickerEntry entry = objectMapper.readValue(line, WikitickerEntry.class);
            entries.add(entry);
        }
        return entries;
    }

    public static DataSource getDataSource() {
        return JdbcSchema.dataSource(DATABASE_URL, org.h2.Driver.class.getName(), "", "");
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

