// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.data.dimension.impl

import com.yahoo.fili.webservice.data.config.dimension.DimensionBackend
import com.yahoo.fili.webservice.data.dimension.DimensionField
import com.yahoo.fili.webservice.data.dimension.DimensionRow
import com.yahoo.fili.webservice.data.dimension.FiliDimensionField
import com.yahoo.fili.webservice.data.dimension.KeyValueStore
import com.yahoo.fili.webservice.data.dimension.MapStoreManager
import com.yahoo.fili.webservice.data.dimension.RedisStoreManager
import com.yahoo.fili.webservice.data.dimension.SearchProvider
import com.yahoo.fili.webservice.web.ApiFilter
import com.yahoo.fili.webservice.web.FilterOperation

import org.joda.time.DateTime

import spock.lang.Shared
import spock.lang.Specification

import java.text.SimpleDateFormat

class KeyValueStoreDimensionSpec extends Specification {

    static KeyValueStoreDimension kvsDimension
    static SearchProvider searchProvider

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
            setupKVStore()
            runSetup = false
        }
    }

    def setupKVStore() {
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(FiliDimensionField.ID)
        dimensionFields.add(FiliDimensionField.DESC)

        String fileName = "dimension_" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
        KeyValueStore keyValueStore
        switch (dimensionBackend) {
            case DimensionBackend.REDIS:
                keyValueStore = RedisStoreManager.getInstance(fileName)
                break
            case DimensionBackend.MEMORY:
                keyValueStore = MapStoreManager.getInstance(fileName)
                break
        }


        kvsDimension = new KeyValueStoreDimension("platform", "platform-description", dimensionFields, keyValueStore, ScanSearchProviderManager.getInstance("platform"))
        kvsDimension.setLastUpdated(lastUpdated)

        dimensionRow1 = FiliDimensionField.makeDimensionRow(kvsDimension, "row1", "this is a row")
        dimensionRow2 = FiliDimensionField.makeDimensionRow(kvsDimension, "row2", "this is a row")
        dimensionRow3 = FiliDimensionField.makeDimensionRow(kvsDimension, "row3", "this is a row3")
        dimensionRowPartial = FiliDimensionField.makeDimensionRow(kvsDimension, "rowPartial", "")

        kvsDimension.addDimensionRow(dimensionRow1)
        kvsDimension.addDimensionRow(dimensionRow2)
        kvsDimension.addDimensionRow(dimensionRow3)

        searchProvider = kvsDimension.getSearchProvider()
    }

    def "Parse partial dimension row succeeds"() {
        setup:
        Map m = ["id": "rowPartial"]

        expect:
        kvsDimension.parseDimensionRow(m) == dimensionRowPartial
    }

    def "Parse normal dimension row succeeds"() {
        setup:
        Map m = ["id": "row1", "desc": "this is a row"]

        expect:
        kvsDimension.parseDimensionRow(m) == dimensionRow1
    }

    def "findAllDimensionRows returns all rows"() {
        expect:
        searchProvider.findAllDimensionRows() == [dimensionRow1, dimensionRow2, dimensionRow3] as Set
    }

    def "getDimensionCardinality returns cardinality count"() {
        expect:
        searchProvider.getDimensionCardinality() == 3
    }

    def "add new DimensionRows"() {
        setup:
        DimensionRow dimensionRow4 = FiliDimensionField.makeDimensionRow(kvsDimension, "row4", "this is a row4")
        DimensionRow dimensionRow5 = FiliDimensionField.makeDimensionRow(kvsDimension, "row5", "this is a row5")
        Set<DimensionRow> dimensionRowSet = [dimensionRow4, dimensionRow5] as Set
        kvsDimension.addAllDimensionRows(dimensionRowSet)

        expect:
        searchProvider.findAllDimensionRows() == [dimensionRow1, dimensionRow2, dimensionRow3, dimensionRow4, dimensionRow5] as Set
    }

    def "Change DimensionRow desc"() {
        Set<ApiFilter> expectedFilters = [new ApiFilter(
                kvsDimension,
                FiliDimensionField.ID,
                FilterOperation.in,
                ["row6"] as Set
        )] as Set

        when:
        DimensionRow dimensionRow6 = FiliDimensionField.makeDimensionRow(kvsDimension, "row6", "foo")
        kvsDimension.addDimensionRow(dimensionRow6)

        dimensionRow6 = FiliDimensionField.makeDimensionRow(kvsDimension, "row6", "value")
        kvsDimension.addDimensionRow(dimensionRow6)

        then:
        searchProvider.findFilteredDimensionRows(expectedFilters) == [dimensionRow6] as Set
    }


    def "getLastUpdated is correct"() {
        expect:
        kvsDimension.getLastUpdated() == lastUpdated
    }

    def "setLastUpdated Not Null"() {
        final DateTime newLastUpdated = new DateTime(20000)

        when: "Not Null"
        kvsDimension.setLastUpdated(newLastUpdated)

        then:
        kvsDimension.getLastUpdated() == newLastUpdated
    }

    def "setLastUpdated Null"() {
        when: "Null"
        kvsDimension.setLastUpdated(null)

        then:
        kvsDimension.getLastUpdated() == null
    }

    class TestThread extends Thread {
        Throwable cause = null

        @Override
        public void run() {
            try {
                final DateTime newLastUpdated = new DateTime(30000)
                kvsDimension.setLastUpdated(newLastUpdated)
                Thread.sleep(100)
                kvsDimension.setLastUpdated(null)
            } catch (Throwable t) {
                cause = t
            }
        }
    }

    def "MultiThread access"() {

        when:
        List<TestThread> threads = [ new TestThread(), new TestThread(), new TestThread() ]

        then:
        threads.each { it.start() }
        threads.each { it.join(10000) }
        threads.each { if ( it.cause != null ) throw it.cause }
    }
}
