// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl

import com.yahoo.bard.webservice.data.cache.HashDataCache
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.util.Pagination
import com.yahoo.bard.webservice.web.util.PaginationParameters

import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path

class MapSearchProviderSpec extends Specification {

    MapSearchProvider sp

    Map<String, DimensionRow> baseRows
    TreeSet<DimensionRow> expectedRows

    def setup() {
        baseRows = testDimensionRows
        expectedRows = new TreeSet(baseRows.values())
        sp = new MapSearchProvider(baseRows)
    }

    def makeSimpleDimensionRow(String id, String desc) {
        return new DimensionRow(
                BardDimensionField.ID,
                [
                        (BardDimensionField.ID) : id,
                        (BardDimensionField.DESC) : desc
                ] as Map
        )
    }

    def getTestDimensionRows() {

        [
                ("Los Angeles") : makeSimpleDimensionRow("Los Angeles", "City of Angels"),
                ("Chicago") : makeSimpleDimensionRow("Chicago", "The Windy City"),
                ("New York") : makeSimpleDimensionRow("New York", "The Big Apple")
        ] as LinkedHashMap
    }

    def "search provider returns all exactly the dimension rows it is initialized with when queried"() {
        expect:
        sp.findAllDimensionRows() == expectedRows

        and:
        sp.findAllOrderedDimensionRows() == expectedRows

        when:
        Pagination<DimensionRow> rowsPaged = sp.findAllDimensionRowsPaged(new PaginationParameters(2, 1))
        Iterator<DimensionRow> iter = expectedRows.iterator()

        then:
        rowsPaged.getPageOfData() == [iter.next(), iter.next()] as List
    }

    def "no mutable search provider methods mutate the map search provider"() {
        when: "refresh index is not supported and doesn't mutate the search provider"
        String key = "Los Angeles"
        DimensionRow oldDimensionRow = baseRows.get(key)
        DimensionRow updatedDimensionRow = DimensionRow.copyWithReplace(
                oldDimensionRow,
                { field, value -> field == BardDimensionField.DESC ? "L.A" : value }
        )
        updatedDimensionRow.put(BardDimensionField.DESC, "L.A.")
        Map<String, HashDataCache.Pair<DimensionRow, DimensionRow>> updatedRowMap = [
                (key): new HashDataCache.Pair(updatedDimensionRow, oldDimensionRow)
        ] as Map
        DimensionRow newDimensionRow = makeSimpleDimensionRow("Bangalore", "Silicon Valley of India")

        and:
        sp.refreshIndex(key, updatedDimensionRow, oldDimensionRow)

        then:
        thrown(UnsupportedOperationException)
        sp.findAllDimensionRows() == expectedRows

        when:
        sp.refreshIndex(key, newDimensionRow, null)

        then:
        thrown(UnsupportedOperationException)
        sp.findAllDimensionRows() == expectedRows

        when:
        sp.refreshIndex(updatedRowMap)

        then:
        thrown(UnsupportedOperationException)
        sp.findAllDimensionRows() == expectedRows

        when: "clear dimension is not support and doesn't mutate search provider"
        sp.clearDimension()

        then:
        thrown(UnsupportedOperationException)
        sp.findAllDimensionRows() == expectedRows

        when: "replace search provider is not supported"
        Path tempFile = Files.createTempFile("map_sp_test", null)
        sp.replaceIndex(tempFile.toString())

        then:
        thrown(UnsupportedOperationException)

        cleanup:
        Files.delete(tempFile)
    }
}
