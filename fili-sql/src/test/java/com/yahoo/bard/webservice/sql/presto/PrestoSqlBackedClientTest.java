// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.presto;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PrestoSqlBackedClientTest {
    String rawSqlQuery;
    String expectedPrestoQuery;

    public PrestoSqlBackedClientTest(String rawSqlQuery, String expectedPrestoQuery) {
        this.rawSqlQuery = rawSqlQuery;
        this.expectedPrestoQuery = expectedPrestoQuery;
    }

    @Parameterized.Parameters
    public static Collection dataProvider() {
        return Arrays.asList(new Object[][] {
                { "SELECT \"dimension1\",\n" +
                        "    \"YEAR\",\n" +
                        "    \"WEEK\", \n" +
                        "    \"metricField1\"\n" +
                        "FROM (\n" +
                        "    SELECT \"dimension1\",\n" +
                        "        YEAR(\"datestamp\") AS \"YEAR\",\n" +
                        "        WEEK(\"datestamp\") AS \"WEEK\",\n" +
                        "        SUM(\"metric1\") AS \"metricField1\"\n" +
                        "    FROM \"colo\".\"db\".\"table\"\n" +
                        "    WHERE \"datestamp\" >= '2021020100' AND \n" +
                        "        \"datestamp\" < '2021020800' AND\n" +
                        "        (CAST(\"dimension1\" AS VARCHAR CHARACTER SET \"ISO-8859-1\") = 'specificValue')\n" +
                        "    GROUP BY \"dimension1\", YEAR(\"datestamp\"), WEEK(\"datestamp\")\n" +
                        "    ORDER BY YEAR(\"datestamp\") NULLS FIRST,\n" +
                        "        WEEK(\"datestamp\") NULLS FIRST,\n" +
                        "        \"dimension1\" NULLS FIRST\n" +
                        "    FETCH NEXT 1 ROWS ONLY) AS \"t4\"",
                "SELECT \"dimension1\",\n" +
                        "    \"YEAR\",\n" +
                        "    \"WEEK\", \n" +
                        "    \"metricField1\"\n" +
                        "FROM (\n" +
                        "    SELECT \"dimension1\",\n" +
                        "        YEAR(\"datestamp\") AS \"YEAR\",\n" +
                        "        WEEK(\"datestamp\") AS \"WEEK\",\n" +
                        "        SUM(\"metric1\") AS \"metricField1\"\n" +
                        "    FROM \"colo\".\"db\".\"table\"\n" +
                        "    WHERE \"datestamp\" >= '2021020100' AND \n" +
                        "        \"datestamp\" < '2021020800' AND\n" +
                        "        (CAST(\"dimension1\" AS VARCHAR CHARACTER SET \"ISO-8859-1\") = 'specificValue')\n" +
                        "    GROUP BY \"dimension1\", YEAR(\"datestamp\"), WEEK(\"datestamp\")\n" +
                        "    ORDER BY YEAR(\"datestamp\") NULLS FIRST,\n" +
                        "        WEEK(\"datestamp\") NULLS FIRST,\n" +
                        "        \"dimension1\" NULLS FIRST\n" +
                        "    LIMIT 1) AS \"t4\""},
                { "SELECT \"source\",\n" +
                        "    YEAR(\"datestamp\") AS \"YEAR\",\n" +
                        "    DAYOFYEAR(\"datestamp\") AS \"DAYOFYEAR\",\n" +
                        "    HOUR(\"datestamp\") AS \"HOUR\",\n" +
                        "    COUNT(*) AS \"count(filterDimension=None,filterType=contains,filterValues=None)\"\n" +
                        "FROM \"colo\".\"db\".\"table\"\n" +
                        "WHERE \"datestamp\" >= '2021020723' AND \"datestamp\" < '2021020823'\n" +
                        "GROUP BY \"source\", YEAR(\"datestamp\"), DAYOFYEAR(\"datestamp\"), HOUR(\"datestamp\")\n" +
                        "ORDER BY YEAR(\"datestamp\") NULLS FIRST, DAYOFYEAR(\"datestamp\") NULLS FIRST, HOUR(\"datestamp\") NULLS FIRST, \"source\" NULLS FIRST\n" +
                        "FETCH NEXT 2 ROWS ONLY",
                "SELECT \"source\",\n" +
                        "    YEAR(\"datestamp\") AS \"YEAR\",\n" +
                        "    DAYOFYEAR(\"datestamp\") AS \"DAYOFYEAR\",\n" +
                        "    HOUR(\"datestamp\") AS \"HOUR\",\n" +
                        "    COUNT(*) AS \"count(filterDimension=None,filterType=contains,filterValues=None)\"\n" +
                        "FROM \"colo\".\"db\".\"table\"\n" +
                        "WHERE \"datestamp\" >= '2021020723' AND \"datestamp\" < '2021020823'\n" +
                        "GROUP BY \"source\", YEAR(\"datestamp\"), DAYOFYEAR(\"datestamp\"), HOUR(\"datestamp\")\n" +
                        "ORDER BY YEAR(\"datestamp\") NULLS FIRST, DAYOFYEAR(\"datestamp\") NULLS FIRST, HOUR(\"datestamp\") NULLS FIRST, \"source\" NULLS FIRST\n" +
                        "LIMIT 2"}
        });
    }


    @Test
    public void testFetchToLimitHelper() {
        assertEquals(expectedPrestoQuery, PrestoSqlBackedClient.fetchToLimitHelper(rawSqlQuery));
    }
}
