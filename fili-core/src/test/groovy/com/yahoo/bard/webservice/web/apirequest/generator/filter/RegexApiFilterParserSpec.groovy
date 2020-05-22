// Copyright 2020 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.filter

import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.metadata.TestDimension
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadFilterException

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class RegexApiFilterParserSpec extends Specification {

    DimensionDictionary dimensionDictionary
    ApiFilterParser parser

    @Shared String dimension1Name = "dimensionOne"
    Dimension dimension1

    @Shared String dimension2Name = "dimensionTwo"
    Dimension dimension2

    def setup() {
        dimension1 = new TestDimension(dimension1Name)
        dimension1.addDimensionField(DefaultDimensionField.ID)

        dimension2 = new TestDimension(dimension2Name)
        dimension2.addDimensionField(DefaultDimensionField.ID)

        dimensionDictionary = new DimensionDictionary()
        dimensionDictionary.add(dimension1)
        dimensionDictionary.add(dimension2)

        parser = new RegexApiFilterParser()

    }

    // Spec tests to write
    //  * building API filter from definition works
    //     - errors thrown on bad defintion. Test all 3 possible pieces where an error can be thrown

    @Shared FilterDefinition expectedDefinition1 = new FilterDefinition(dimension1Name, "id", "in", ["1"])
    @Shared FilterDefinition expectedDefinition2 = new FilterDefinition(dimension1Name, "id", "in", ["1", "2", "345"])
    @Shared FilterDefinition expectedDefinition3 = new FilterDefinition(dimension1Name, "id", "in", ["1", "3|4|5"])
    @Shared FilterDefinition expectedDefinition4 = new FilterDefinition(dimension1Name, "id", "in", ["1", "34,5"])
    @Shared FilterDefinition expectedDefinition5 = new FilterDefinition(dimension1Name, "id", "in", ["1", "3(😃)5"])
    @Shared FilterDefinition expectedDefinition6 = new FilterDefinition(dimension1Name, "id", "in", ["dimensionOne|id-in"])
    @Shared FilterDefinition expectedDefinition7 = new FilterDefinition(dimension1Name, "_id_", "00_in", ["1", "2"])

    @Unroll
    def "Parser produces correct value for single filter"() {
        expect:
        parser.parseApiFilterQuery(text) == expected

        // TODO what other cases should I add here? This should be a COMPLETE set of cases for correctly formatted single filters
        where:
        text                                     || expected
        null                                     || []
        ""                                       || []
        "dimensionOne|id-in[1]"                  || [expectedDefinition1]
        "dimensionOne|id-in[1,2,345]"            || [expectedDefinition2]
        "dimensionOne|id-in[1,3|4|5]"            || [expectedDefinition3]
        "dimensionOne|id-in[1,\"34,5\"]"          || [expectedDefinition4]
        "dimensionOne|id-in[1,3(😃)5]"           || [expectedDefinition5]
        "dimensionOne|id-in[dimensionOne|id-in]" || [expectedDefinition6]
        "dimensionOne|_id_-00_in[1,2]"           || [expectedDefinition7]
    }

    def "Multiple filters are all parsed properly"() {
        given:
        String complexFilterQuery = "dimensionOne|id-in[1,2,345],dimensionOne|id-notin[6,7,8],dimensionTwo|id-equals[910]"

        // all filters are parsed separately. Combining and merging filters is handled elsewhere
        List<FilterDefinition> expected = [
                new FilterDefinition(dimension1Name, "id", "in", ["1", "2", "345"]),
                new FilterDefinition(dimension1Name, "id", "notin", ["6", "7", "8"]),
                new FilterDefinition(dimension2Name, "id", "equals", ["910"]),
        ]

        expect:
        parser.parseApiFilterQuery(complexFilterQuery) == expected
    }

    @Unroll
    def "badly formatted filter throws errors"() {
        when:
        parser.parseApiFilterQuery(text)

        then:
        thrown(BadFilterException)

        where:
        text                                                | _
        "dimensionOne"                                      | _
        "dimension-in[filterValue]"                         | _
        "dimension|id-[filterValue]"                        | _
        "dimension|idin[filterValue]"                       | _
        "dimension|id-infilterValue]"                       | _
        "dimension|id-in[filterValue]]"                     | _
        "dimension|id-in[filterVa]lue"                      | _
        "dimension|id-in[]"                                 | _
        "dimension|id-in[],]"                               | _
        "dimension|id-in[filterVa],dim|id-in[filterVal]]"   | _
    }
}
