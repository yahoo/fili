package com.yahoo.bard.webservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.mock.WikitickerEntry;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hinterlong on 5/30/17.
 */
public class Database {
    private static final String DATABASE_URL = "jdbc:h2:mem:test";

    public static Connection getDatabase() throws Exception {
        Connection connection = DriverManager.getConnection(DATABASE_URL);
        Statement s = connection.createStatement();
        List<WikitickerEntry> entries = readJsonFile();

        s.execute("CREATE TABLE WIKITICKER (ID INT PRIMARY KEY," +
                "COMMENT VARCHAR(256)," +
                "COUNTRY_ISO_CODE VARCHAR(256)," +
                "ADDED INTEGER," +
                "TIME VARCHAR(256), " +
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

        String sqlInsert = "INSERT INTO WIKITICKER (ID, COMMENT, COUNTRY_ISO_CODE, ADDED, TIME, IS_NEW, IS_ROBOT, DELETED, METRO_CODE, IS_UNPATROLLED, NAMESPACE, PAGE, COUNTRY_NAME, CITY_NAME, IS_MINOR, USER, DELTA, IS_ANONYMOUS, REGION_ISO_CODE, CHANNEL, REGION_NAME) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        int i = 0;
        for (WikitickerEntry e : entries) {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlInsert);
            preparedStatement.setInt(1, i);
            preparedStatement.setString(2, e.getComment());
            preparedStatement.setString(3, e.getCountryIsoCode());
            preparedStatement.setInt(4, e.getAdded());
            preparedStatement.setString(5, e.getTime());
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
        String filename = "wikiticker-2015-09-12-sampled.json";
        ClassLoader classLoader = Database.class.getClassLoader();
        File file = new File(classLoader.getResource(filename).getFile());

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
     * print basic column info about the result set
     *
     * @param rsmd
     * @throws SQLException
     */
    public static void printColTypes(ResultSetMetaData rsmd) throws SQLException {
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            int jdbcType = rsmd.getColumnType(i);
            String name = rsmd.getColumnName(i);
            String type = rsmd.getColumnTypeName(i);
            System.out.printf("%10s[%s-%d]", name, type, jdbcType);
        }
        System.out.println();
    }
}

