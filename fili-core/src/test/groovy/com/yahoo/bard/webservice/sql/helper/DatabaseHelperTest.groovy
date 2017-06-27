// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.helper

import static com.yahoo.bard.webservice.sql.database.Database.TIME
import static com.yahoo.bard.webservice.sql.database.Database.WIKITICKER

import com.yahoo.bard.webservice.sql.database.Database

import spock.lang.Specification

import java.sql.Connection

class DatabaseHelperTest extends Specification {
    static Connection connection = Database.initializeDatabase()
    static CalciteHelper calciteHelper = new CalciteHelper(Database.getDataSource(), CalciteHelper.DEFAULT_SCHEMA)

    def "GetTimestampColumn from table"() {
        setup:
        String tableWithSchema = calciteHelper.escapeTableName(WIKITICKER)

        expect:
        TIME == DatabaseHelper.getTimestampColumn(connection, tableWithSchema)
    }
}
