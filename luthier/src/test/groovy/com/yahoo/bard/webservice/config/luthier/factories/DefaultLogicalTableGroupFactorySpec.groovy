// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories

import com.yahoo.bard.webservice.application.LuthierBinderFactory
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark
import com.yahoo.bard.webservice.data.time.AllGranularity
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.TableIdentifier
import org.joda.time.DateTimeZone
import org.joda.time.Period
import spock.lang.Specification

class DefaultLogicalTableGroupFactorySpec extends Specification {
    LuthierBinderFactory binderFactory
    LuthierIndustrialPark park
    LogicalTable wikipediaDayTable
    PhysicalTable wikiticker
    ZonelessTimeGrain day = DefaultTimeGrain.DAY
    ZonelessTimeGrain hour = DefaultTimeGrain.HOUR
    AllGranularity all = AllGranularity.INSTANCE
    ZonedTimeGrain expectedGrain = day.buildZonedTimeGrain(DateTimeZone.UTC)
    Map tableDictionary
    void setup() {
        binderFactory = new LuthierBinderFactory()              // must go through the binder factory to correctly
                                                                // configure the industrial park > GranularityDictionary
        park = (LuthierIndustrialPark) binderFactory.getConfigurationLoader()
        park.load()
    }

    def "A specific LogicalTable exists in the loaded Table Dictionary, and contains correct info"() {
        when:
            wikipediaDayTable = park.getLogicalTable(new TableIdentifier("wikipedia", day))
            wikiticker = park.getPhysicalTable("wikiticker")
        then:
            wikipediaDayTable.getLongName() == "wikipedia logical table"
            wikipediaDayTable.getCategory() == "wikipedia category"
            wikipediaDayTable.getDescription() == "wikipedia description"
            wikipediaDayTable.getRetention() == Period.parse("P2Y")
            wikipediaDayTable.getDimensions() == wikiticker.getDimensions()
            wikipediaDayTable.getGranularity() == expectedGrain
            wikipediaDayTable.getDimensions() == wikiticker.getDimensions()
            wikipediaDayTable.getLogicalMetrics().empty         // To be finished when LogicalMetrics are implemented
            // transitively test on physicalTable content correctness
            wikipediaDayTable.tableGroup.physicalTables.contains(wikiticker)
    }

    def "The logicalTableDictionary contains the correct set of TableIdentifier keys"() {
        when:
            tableDictionary = park.getLogicalTableDictionary()
        then:
            tableDictionary.size() == 6
            tableDictionary.containsKey(new TableIdentifier("wikipedia", day))
            tableDictionary.containsKey(new TableIdentifier("wikipedia", hour))
            tableDictionary.containsKey(new TableIdentifier("wikipedia", all))
            tableDictionary.containsKey(new TableIdentifier("air_quality", day))
            tableDictionary.containsKey(new TableIdentifier("air_quality", hour))
            tableDictionary.containsKey(new TableIdentifier("air_quality", all))
    }
}
