// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories

import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark
import com.yahoo.bard.webservice.data.config.luthier.LuthierResourceDictionaries
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.table.PhysicalTable
import spock.lang.Specification

class StrictPhysicalTableFactorSpec extends Specification {
    LuthierIndustrialPark park
    PhysicalTable wikitickerTable
    PhysicalTable airTable
    Map<String, PhysicalTable> tableDictionary
    Dimension expectedTestDimension
    void setup() {
        LuthierResourceDictionaries resourceDictionaries = new LuthierResourceDictionaries()
        park = new LuthierIndustrialPark.Builder(resourceDictionaries).build()
        park.load()
        tableDictionary = park.getPhysicalTableDictionary()
    }

    def "physical table dictionary contains correct keys"() {
        when:
            park.getPhysicalTable("air")
            for (physicalTableName in park.physicalTableFactoryPark.fetchConfig().fieldNames()) {
                park.getPhysicalTable(physicalTableName)
            }
        then:
            tableDictionary.size() == 2
            tableDictionary.containsKey("wikiticker")
            tableDictionary.containsKey("air")
            ! tableDictionary.containsKey("NON_EXISTENT_TABLE")
    }

    def "check that a specific dimension is loaded into the correct table"() {
        when:
            wikitickerTable = park.getPhysicalTable("wikiticker")
            airTable = park.getPhysicalTable("air")
            expectedTestDimension = park.getDimension("testDimension")
        then:
            ! wikitickerTable.getDimensions().contains(expectedTestDimension)
            ! airTable.getDimensions().contains(expectedTestDimension)
            wikitickerTable.getPhysicalColumnName("testDimension") == "testDimensionPhysicalName"
            wikitickerTable.getPhysicalColumnName("testDimension") != "wrongPhysicalName"
            ! wikitickerTable.getSchema().getLogicalColumnNames("testDimensionPhysicalName").contains("wrongLogicalName")
            wikitickerTable.getSchema().getLogicalColumnNames("testDimensionPhysicalName").contains("testDimension")
    }

    def "general information is correct in the wikiticker table"() {
        when:
            wikitickerTable = park.getPhysicalTable("wikiticker")
        then:
            wikitickerTable.getSchema().getTimeGrain().getTimeZoneName() == "UTC"
            wikitickerTable.getSchema().getTimeGrain().getName() == "hour"
    }
}
