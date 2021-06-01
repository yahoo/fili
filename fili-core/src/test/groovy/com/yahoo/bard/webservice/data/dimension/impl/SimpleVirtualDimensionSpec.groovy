// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl

import com.yahoo.bard.webservice.data.config.dimension.DimensionBackend
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.DefaultFilterOperation

import org.joda.time.DateTime

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject

import java.text.SimpleDateFormat

class SimpleVirtualDimensionSpec extends Specification {

    @Subject
    static SimpleVirtualDimension simpleVirtualDimension

    DimensionBackend dimensionBackend

    @Shared
    DateTime lastUpdated = new DateTime(10000)

    @Shared
    DimensionRow dimensionRow1
    @Shared
    DimensionRow dimensionRow2
    @Shared
    DimensionRow dimensionRow3

    DimensionRow dimensionRowPartial
    @Shared
    def runSetup = true

    def setup() {
        dimensionBackend = DimensionBackend.getBackend();
        if (runSetup || DimensionBackend.MEMORY == dimensionBackend) {
            setupTestData()
            runSetup = false
        }
    }

    def setupTestData() {
        LinkedHashSet<DimensionField> dimensionFields = SimpleVirtualDimension.FIELDS

        String fileName = "dimension_" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

        simpleVirtualDimension = new SimpleVirtualDimension("platform")

        dimensionRow1 = BardDimensionField.makeDimensionRow(simpleVirtualDimension, "row1")
        dimensionRow2 = BardDimensionField.makeDimensionRow(simpleVirtualDimension, "row2")
        dimensionRow3 = BardDimensionField.makeDimensionRow(simpleVirtualDimension, "row3");
        dimensionRowPartial = BardDimensionField.makeDimensionRow(simpleVirtualDimension, "rowPartial")
    }

    def "Parse partial dimension row succeeds"() {
        setup:
        Map m = ["": "rowPartial"]

        expect:
        simpleVirtualDimension.parseDimensionRow(m) == dimensionRowPartial
    }

    def "Parse empty dimension row fails"() {
        setup:
        Map m = [:]

        when:
        simpleVirtualDimension.parseDimensionRow(m)

        then:
        thrown(IllegalArgumentException)
    }

    def "Parse dimension row with bad fields fails"() {
        setup:
        Map m = ["": "one", "illegal": "fail"]

        when:
        simpleVirtualDimension.parseDimensionRow(m)

        then:
        thrown(IllegalArgumentException)
    }

    def "Parse normal dimension row succeeds"() {
        setup:
        Map m = ["": "row1"]

        expect:
        simpleVirtualDimension.parseDimensionRow(m) == dimensionRow1
    }

    def "add new DimensionRows isn't supported"() {
        setup:
        DimensionRow dimensionRow4 = BardDimensionField.makeDimensionRow(simpleVirtualDimension, "row4")
        DimensionRow dimensionRow5 = BardDimensionField.makeDimensionRow(simpleVirtualDimension, "row5")
        Set<DimensionRow> dimensionRowSet = [dimensionRow4, dimensionRow5] as Set
        when:
        simpleVirtualDimension.addAllDimensionRows(dimensionRowSet)

        then:
        thrown(UnsupportedOperationException)
    }

    def "getLastUpdated is always null"() {
        expect:
        simpleVirtualDimension.getLastUpdated() == null
    }

    def "setLastUpdated is no supported"() {
        final DateTime newLastUpdated = new DateTime(20000)

        when: "Not Null"
        simpleVirtualDimension.setLastUpdated(newLastUpdated)

        then:
        thrown(UnsupportedOperationException)
    }
}
