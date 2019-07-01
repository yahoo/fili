// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories

import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark
import com.yahoo.bard.webservice.data.config.LuthierResourceDictionaries
import com.yahoo.bard.webservice.table.PhysicalTable
import spock.lang.Specification

class StrictPhysicalTableFactorSpec extends Specification {
    LuthierIndustrialPark park
    PhysicalTable wikitickerTable
    Map<String, PhysicalTable> tableDictionary
    void setup() {
        LuthierResourceDictionaries resourceDictionaries = new LuthierResourceDictionaries()
        park = new LuthierIndustrialPark.Builder(resourceDictionaries).build()
        park.load()
        wikitickerTable = park.getPhysicalTable("wikiticker")
        tableDictionary = park.getPhysicalTableDictionary()
    }

    def "physical table dictionary contains correct keys"() {
        expect:
            tableDictionary.size() == 2
            tableDictionary.containsKey("wikiticker")
            tableDictionary.containsKey("air")
            ! tableDictionary.containsKey("NON_EXISTENT_TABLE")
    }
}
