// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.building

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_GRANULARITY_MISMATCH

import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.web.BadApiRequestException
import com.yahoo.bard.webservice.web.apirequest.binders.LogicalTableGenerator

import spock.lang.Specification

class DefaultLogicalTableGeneratorSpec extends Specification {
    def "generateTable() returns existing LogicalTable"() {
        given: "we insert a LogicalTable into LogicalTableDictionary"
        String tableName = "tableName"
        Granularity granularity = Mock(Granularity)
        LogicalTableDictionary logicalTableDictionary = Mock(LogicalTableDictionary)
        TableIdentifier tableIdentifier = new TableIdentifier(tableName, granularity)
        LogicalTable logicalTable = Mock(LogicalTable)
        logicalTableDictionary.get(tableIdentifier) >> logicalTable

        expect: "we can fetch the LogicalTable from it"
        LogicalTableGenerator.DEFAULT_LOGICAL_TABLE_GENERATOR.generateTable(tableName, granularity, logicalTableDictionary) == logicalTable
    }

    def "generateTable() throws BadApiRequestException on non-existing LogicalTable"() {
        given: "we insert a LogicalTable into LogicalTableDictionary"
        String tableName = "tableName"
        Granularity granularity = Mock(Granularity)
        LogicalTableDictionary logicalTableDictionary = Mock(LogicalTableDictionary)
        TableIdentifier tableIdentifier = new TableIdentifier(tableName, granularity)
        LogicalTable logicalTable = Mock(LogicalTable)
        logicalTableDictionary.get(tableIdentifier) >> logicalTable

        String nonExistingTableName = "I don't exist"

        when: "we try to fetch a non-existing LogicalTable"
        LogicalTableGenerator.DEFAULT_LOGICAL_TABLE_GENERATOR.generateTable(nonExistingTableName, granularity, logicalTableDictionary)

        then: "we get a BadApiRequestException"
        BadApiRequestException exception = thrown()
        exception.message == TABLE_GRANULARITY_MISMATCH.logFormat(granularity, nonExistingTableName)
    }
}
