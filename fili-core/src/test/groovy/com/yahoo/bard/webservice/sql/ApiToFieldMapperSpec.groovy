// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql

import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.getDictionary
import static com.yahoo.bard.webservice.sql.database.Database.WIKITICKER

import spock.lang.Specification

class ApiToFieldMapperSpec extends Specification {
    static ApiToFieldMapper aliasMaker
    static List<String> allColumns

    def setup() {
        def schema = getDictionary("api_", "field_").get(WIKITICKER).getSchema()
        allColumns = schema.columnNames as List

        aliasMaker = new ApiToFieldMapper(schema)
    }

    def "Apply"() {
        expect:
        allColumns.forEach {
            aliasMaker.apply("api_" + it) == "field_" + it
        }
    }

    def "UnApply"() {
        expect:
        allColumns.forEach {
            aliasMaker.unApply("field_" + it) == "api_" + it
        }
    }
}
