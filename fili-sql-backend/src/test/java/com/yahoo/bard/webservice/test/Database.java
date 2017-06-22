// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.test;

import static com.yahoo.bard.webservice.SqlConverter.THE_SCHEMA;

import com.yahoo.bard.webservice.TimestampUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

/**
 * Simple, in-memory database with the example wikiticker data loaded.
 */
public class Database {
    private static final String DATABASE_URL = "jdbc:h2:mem:test";
    private static final String WIKITICKER_JSON_DATA = "wikiticker-2015-09-12-sampled.json";
    private static Connection connection;

    /**
     * Gets an in memory database with the {@link WikitickerEntry} from the example data.
     *
     * @return the connection to the database.
     *
     * @throws SQLException if can't create database correctly.
     * @throws IOException if can't read example data file.
     */
    public static Connection getDatabase() throws SQLException, IOException {
        if (connection == null) {
            connection = DriverManager.getConnection(DATABASE_URL);
        } else {
            return connection;
        }

        List<WikitickerEntry> entries = readJsonFile();

        // todo try to move this out or see if we don't need a schema
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
                    TimestampUtils.timestampFromString(e.getTime())
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

    /**
     * Reads the json file of example data as List of {@link WikitickerEntry}.
     *
     * @return a list of entries.
     *
     * @throws IOException if can't read file.
     */
    public static List<WikitickerEntry> readJsonFile() throws IOException {
        File file = new File(Database.class.getClassLoader().getResource(WIKITICKER_JSON_DATA).getFile());

        String json = FileUtils.readFileToString(file);
        List<WikitickerEntry> entries = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        for (String line : json.split(System.lineSeparator())) {
            WikitickerEntry entry = objectMapper.readValue(line, WikitickerEntry.class);
            entries.add(entry);
        }
        return entries;
    }

    /**
     * Gets a {@link DataSource} of for the wikiticker database.
     *
     * @return the datasource.
     */
    public static DataSource getDataSource() {
        return JdbcSchema.dataSource(DATABASE_URL, org.h2.Driver.class.getName(), "", "");
    }
}
