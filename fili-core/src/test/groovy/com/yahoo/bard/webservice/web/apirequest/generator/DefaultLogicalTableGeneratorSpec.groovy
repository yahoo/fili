// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_GRANULARITY_MISMATCH

import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder
import com.yahoo.bard.webservice.web.util.BardConfigResources

import spock.lang.Specification

class DefaultLogicalTableGeneratorSpec extends Specification {

    Generator<LogicalTable> gen

    def setup() {
        gen = new DefaultLogicalTableGenerator()
    }

    def "bind() returns existing LogicalTable"() {
        setup: "we insert a LogicalTable into LogicalTableDictionary"
        String tableName = "tableName"
        Granularity granularity = Mock(Granularity)
        LogicalTableDictionary logicalTableDictionary = Mock(LogicalTableDictionary)
        TableIdentifier tableIdentifier = new TableIdentifier(tableName, granularity)
        LogicalTable logicalTable = Mock(LogicalTable)
        logicalTableDictionary.get(tableIdentifier) >> logicalTable
        BardConfigResources resources = Mock(BardConfigResources)
        resources.getLogicalTableDictionary() >> logicalTableDictionary

        and: "built the request params"
        TestRequestParameters params = new TestRequestParameters()
        params.logicalTable = tableName

        and: "build the granularity"
        DataApiRequestBuilder builder = new DataApiRequestBuilder(resources)
        builder.setGranularity(params, new TestGenerator<Granularity>(granularity))

        expect: "we can fetch the LogicalTable from it"
        gen.bind(builder, params, resources) == logicalTable
    }

    def "bind() throws BadApiRequestException on non-existing LogicalTable"() {
        given: "we insert a LogicalTable into LogicalTableDictionary"
        String tableName = "tableName"
        Granularity granularity = Mock(Granularity)
        LogicalTableDictionary logicalTableDictionary = Mock(LogicalTableDictionary)
        TableIdentifier tableIdentifier = new TableIdentifier(tableName, granularity)
        LogicalTable logicalTable = Mock(LogicalTable)
        logicalTableDictionary.get(tableIdentifier) >> logicalTable
        BardConfigResources resources = Mock(BardConfigResources)
        resources.getLogicalTableDictionary() >> logicalTableDictionary

        and: "build request params"
        String nonExistingTableName = "I don't exist"
        TestRequestParameters params = new TestRequestParameters()
        params.logicalTable = nonExistingTableName

        and: "built the granularity"
        DataApiRequestBuilder builder = new DataApiRequestBuilder(resources)
        builder.setGranularity(params, new TestGenerator<Granularity>(granularity))

        when: "we try to fetch a non-existing LogicalTable"
        gen.bind(builder, params, resources)

        then: "we get a BadApiRequestException"
        BadApiRequestException exception = thrown()
        exception.message == TABLE_GRANULARITY_MISMATCH.logFormat(granularity, nonExistingTableName)
    }

    def "bind() throws an error if the granularity was not bound before the logical table was generated"() {
        setup: "we insert a LogicalTable into LogicalTableDictionary"
        String tableName = "tableName"
        LogicalTableDictionary logicalTableDictionary = Mock(LogicalTableDictionary)
        TableIdentifier tableIdentifier = new TableIdentifier(tableName, Mock(Granularity))
        LogicalTable logicalTable = Mock(LogicalTable)
        logicalTableDictionary.get(tableIdentifier) >> logicalTable
        BardConfigResources resources = Mock(BardConfigResources)
        resources.getLogicalTableDictionary() >> logicalTableDictionary

        TestRequestParameters params = new TestRequestParameters()
        params.logicalTable = tableName
        DataApiRequestBuilder builder = new DataApiRequestBuilder(resources)

        when:
        gen.bind(builder, params, resources)

        then:
        thrown(UnsatisfiedApiRequestConstraintsException)
    }
}
