// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource

import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.table.ConstrainedTable

import spock.lang.Specification

class UnionDataSourceSpec extends Specification {
    Dimension dimension1
    Dimension dimension2

    ConstrainedTable table

    def setup() {
        dimension1 = Mock(Dimension)
        dimension2 = Mock(Dimension)

        dimension1.getApiName() >> "example"
        dimension2.getApiName() >> "otherExample"

        table = Mock(ConstrainedTable)
        table.getPhysicalColumnName("example") >> "example1"
    }

    def "Test simple construction"() {
        setup:
        Set<String> expectedNames = ["test1", "test2"] as Set
        table.getDataSourceNames() >> (expectedNames.collect {DataSourceName.of(it)} as Set)

        expect:
        new UnionDataSource(table).names == expectedNames
    }
}
