// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.table.PhysicalTable

import spock.lang.Specification

class UnionDataSourceSpec extends Specification {
    Dimension dimension1
    Dimension dimension2

    PhysicalTable table1
    PhysicalTable table2

    def setup() {
        dimension1 = Mock(Dimension)
        dimension2 = Mock(Dimension)

        dimension1.getApiName() >> "example"
        dimension2.getApiName() >> "otherExample"

        table1 = Mock(PhysicalTable)
        table2 = Mock(PhysicalTable)
    }

    def "check duplicate physical mappings for same name fails"() {
        setup:
        table1.getPhysicalColumnName("example") >> "example1"
        table2.getPhysicalColumnName("example") >> "I'm different"
        table1.getDimensions() >> [dimension1]
        table2.getDimensions() >> [dimension1]

        Set<PhysicalTable> tables = [table1, table2]

        when:
        new UnionDataSource(tables)

        then:
        thrown RuntimeException
    }

    def "check proper instantiation with same dimension mapped to same name"() {
        setup:
        table1.getPhysicalColumnName("example") >> "example2"
        table2.getPhysicalColumnName("example") >> "example2"
        table1.getDimensions() >> [dimension1]
        table2.getDimensions() >> [dimension1]

        Set<PhysicalTable> tables = [table1, table2]

        when:
        new UnionDataSource(tables)

        then:
        noExceptionThrown()
    }

    def "check proper instantiation with different dimensions mapped to different names"() {
        setup:
        table1.getDimensions() >> [dimension2]
        table2.getDimensions() >> [dimension1]

        table1.getPhysicalColumnName("otherExample") >> "otherMapping"
        table2.getPhysicalColumnName("example") >> "woot"

        Set<PhysicalTable> tables = [table1, table2]

        when:
        new UnionDataSource(tables)

        then:
        noExceptionThrown()
    }

    def "check multiple non-colliding dimensions map properly"() {
        setup:
        table1.getDimensions() >> [dimension1, dimension2]
        table2.getDimensions() >> [dimension1]

        table1.getPhysicalColumnName("example") >> "woot"
        table1.getPhysicalColumnName("otherExample") >> "otherMapping"
        table2.getPhysicalColumnName("example") >> "woot"

        Set<PhysicalTable> tables = [table1, table2]

        when:
        new UnionDataSource(tables)

        then:
        noExceptionThrown()
    }
}
