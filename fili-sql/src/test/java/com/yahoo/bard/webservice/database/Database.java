// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.database;

import com.yahoo.bard.webservice.sql.helper.TimestampUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.calcite.adapter.jdbc.JdbcSchema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.sql.DataSource;

/**
 * Simple, in-memory database with the example wikiticker data loaded.
 */
public class Database {
    private static final String DATABASE_URL = "jdbc:h2:mem:test";
    public static final String SCHEMA = "PUBLIC";
    private static final String WIKITICKER_JSON_DATA = "wikiticker-2015-09-12-sampled.json";
    private static Connection connection;
    public static final String TIME = "TIME";
    public static final String COUNTRY_ISO_CODE = "countryIsoCode";
    public static final String IS_UNPATROLLED = "isUnpatrolled";
    public static final String NAMESPACE = "namespace";
    public static final String COUNTRY_NAME = "countryName";
    public static final String CITY_NAME = "cityName";
    public static final String IS_MINOR = "isMinor";
    public static final String IS_ANONYMOUS = "isAnonymous";
    public static final String REGION_ISO_CODE = "regionIsoCode";
    public static final String CHANNEL = "channel";
    public static final String REGION_NAME = "regionName";
    public static final String WIKITICKER = "wikiticker";
    public static final String IS_NEW = "isNew";
    public static final String IS_ROBOT = "isRobot";
    public static final String PAGE = "page";
    public static final String USER = "user";
    public static final String COMMENT = "comment";
    public static final String ADDED = "added";
    public static final String DELETED = "deleted";
    public static final String DELTA = "delta";
    public static final String ID = "ID";
    public static final String METRO_CODE = "metroCode";
    public static final String COUNT = "count";

    /**
     * Gets an in memory database with the {@link WikitickerEntry} from the example data.
     *
     * @return the connection to the database.
     *
     * @throws SQLException if can't create database correctly.
     * @throws IOException if can't read example data file.
     */
    public static Connection initializeDatabase() throws SQLException, IOException {
        if (connection == null) {
            connection = DriverManager.getConnection(DATABASE_URL);
        } else {
            return connection;
        }

        List<WikitickerEntry> entries = readJsonFile();

        Statement s = connection.createStatement();
        /*
        Unescaped Readable sql:
        "CREATE TABLE  $WIKITICKER (
            $ID                 INT PRIMARY KEY,
            $COMMENT            VARCHAR(256),
            $COUNTRY_ISO_CODE   VARCHAR(256),
            $ADDED              INTEGER,
            $TIME               TIMESTAMP,
            $IS_NEW             BOOLEAN,
            $IS_ROBOT           BOOLEAN,
            $DELETED            INTEGER,
            $METRO_CODE         VARCHAR(256),
            $IS_UNPATROLLED     BOOLEAN,
            $NAMESPACE          VARCHAR(256),
            $PAGE               VARCHAR(256),
            $COUNTRY_NAME       VARCHAR(256),
            $CITY_NAME          VARCHAR(256),
            $IS_MINOR           BOOLEAN,
            $USER               VARCHAR(256),
            $DELTA              INTEGER,
            $IS_ANONYMOUS       BOOLEAN,
            $REGION_ISO_CODE    VARCHAR(256),
            $CHANNEL            VARCHAR(256),
            $REGION_NAME        VARCHAR(256)
        )"
         */
        s.execute("CREATE TABLE \"" + WIKITICKER + "\" (\"" + ID + "\" INT PRIMARY KEY, \"" +
                COMMENT + "\" VARCHAR(256), \"" +
                COUNTRY_ISO_CODE + "\" VARCHAR(256), \"" +
                ADDED + "\" INTEGER,\"" +
                TIME + "\" TIMESTAMP, \"" +
                IS_NEW + "\" BOOLEAN, \"" +
                IS_ROBOT + "\" BOOLEAN, \"" +
                DELETED + "\" INTEGER, \"" +
                METRO_CODE + "\" VARCHAR(256), \"" +
                IS_UNPATROLLED + "\" BOOLEAN, \"" +
                NAMESPACE + "\" VARCHAR(256), \"" +
                PAGE + "\" VARCHAR(256), \"" +
                COUNTRY_NAME + "\" VARCHAR(256), \"" +
                CITY_NAME + "\" VARCHAR(256), \"" +
                IS_MINOR + "\" BOOLEAN, \"" +
                USER + "\" VARCHAR(256), \"" +
                DELTA + "\" INTEGER, \"" +
                IS_ANONYMOUS + "\" BOOLEAN, \"" +
                REGION_ISO_CODE + "\" VARCHAR(256), \"" +
                CHANNEL + "\" VARCHAR(256), \"" +
                REGION_NAME + "\" VARCHAR(256), " +
                ")"
        );

        /*
        Unescaped readable sql:
        "INSERT INTO $WIKITICKER ($ID, $COMMENT, $COUNTRY_ISO_CODE, $ADDED, $TIME, $IS_NEW, $IS_ROBOT, $DELETED,
        $METRO_CODE, $IS_UNPATROLLED, $NAMESPACE, $PAGE, $COUNTRY_NAME, $CITY_NAME, $IS_MINOR, $USER, $DELTA,
        $IS_ANONYMOUS, $REGION_ISO_CODE, $CHANNEL, $REGION_NAME)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
         */
        String sqlInsert = "INSERT INTO \"" + WIKITICKER + "\" (\"" + ID + "\", \"" + COMMENT + "\", \"" +
                COUNTRY_ISO_CODE + "\", \""
                + ADDED + "\", \"" + TIME + "\", \"" + IS_NEW + "\", \"" + IS_ROBOT + "\", \"" + DELETED + "\", \"" +
                METRO_CODE + "\", \"" +
                IS_UNPATROLLED + "\", \"" + NAMESPACE + "\", \"" + PAGE + "\", \"" + COUNTRY_NAME + "\", \"" +
                CITY_NAME + "\", \"" +
                IS_MINOR + "\", \"" + USER + "\", \"" + DELTA + "\", \"" + IS_ANONYMOUS + "\", \"" + REGION_ISO_CODE
                + "\", \"" + CHANNEL
                + "\", \"" + REGION_NAME + "\") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
        List<WikitickerEntry> entries = new ArrayList<>();
        try (InputStream wikiData = Database.class.getClassLoader().getResourceAsStream(WIKITICKER_JSON_DATA)) {
            List<String> wikiDataLines =
                    new BufferedReader(new InputStreamReader(
                            wikiData,
                            StandardCharsets.UTF_8
                    )).lines().collect(Collectors.toList());
            ObjectMapper objectMapper = new ObjectMapper();
            for (String line : wikiDataLines) {
                WikitickerEntry entry = objectMapper.readValue(line, WikitickerEntry.class);
                entries.add(entry);
            }
        }

        return entries;
    }

    /**
     * Gets a {@link DataSource} of for the wikiticker database.
     *
     * @return the datasource.
     *
     * @throws IOException if can't read wikiticker data file while initializing.
     * @throws SQLException if can't create datasource.
     */
    public static DataSource getDataSource() throws IOException, SQLException {
        initializeDatabase();
        return JdbcSchema.dataSource(DATABASE_URL, org.h2.Driver.class.getName(), "", "");
    }
}
