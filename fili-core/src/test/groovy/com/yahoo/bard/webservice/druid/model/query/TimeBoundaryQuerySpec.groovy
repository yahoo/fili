// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import org.joda.time.DateTimeZone

import spock.lang.Specification

class TimeBoundaryQuerySpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    def "TimeBoundaryQuery serializes to JSON correctly"() {
        given: "A Table data source"
        String tableName = "basefact_network"
        TableDataSource dataSource = new TableDataSource(new PhysicalTable(tableName, DefaultTimeGrain.DAY.buildZonedTimeGrain(DateTimeZone.UTC), [:]))

        when: "We create and serialize a TimeBoundaryQuery"
        String timeBoundaryQueryStr = MAPPER.writeValueAsString(new TimeBoundaryQuery(dataSource))

        then: "The serialized JSON is what we expect"
        String expectedQuery = """
            {
                "queryType": "timeBoundary",
                "dataSource": {
                    "type": "table",
                    "name": "$tableName"
                },
                "context": {}
            }"""
        GroovyTestUtils.compareJson(timeBoundaryQueryStr, expectedQuery)
    }
}
