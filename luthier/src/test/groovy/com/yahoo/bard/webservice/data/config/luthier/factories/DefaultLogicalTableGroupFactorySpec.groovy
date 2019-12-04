// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories

import com.yahoo.bard.webservice.application.LuthierBinderFactory
import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark
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
    ZonelessTimeGrain day = DefaultTimeGrain.DAY
    ZonelessTimeGrain hour = DefaultTimeGrain.HOUR
    AllGranularity all = AllGranularity.INSTANCE
    Map tableDictionary
    void setup() {
        binderFactory = new LuthierBinderFactory()              // must go through the binder factory to correctly
                                                                // configure the industrial park > GranularityDictionary
        park = (LuthierIndustrialPark) binderFactory.getConfigurationLoader()
        park.load()
    }

    def "The wikipedia (day) LogicalTable exists in the loaded Table Dictionary, and contains correct info"() {
        when:
            LogicalTable wikipediaDayTable = park.getLogicalTable(new TableIdentifier("wikipedia", day))
            ZonedTimeGrain expectedZonedDayGrain = day.buildZonedTimeGrain(DateTimeZone.UTC)
            PhysicalTable wikiticker = park.getPhysicalTable("wikiticker")
            List<String> expectedMetricNames = Arrays.asList("count", "added", "delta", "deleted")
        then:
            wikipediaDayTable.getLongName() == "wikipedia logical table"
            wikipediaDayTable.getCategory() == "wikipedia category"
            wikipediaDayTable.getDescription() == "wikipedia description"
            wikipediaDayTable.getRetention() == Period.parse("P2Y")
            wikipediaDayTable.getDimensions() == wikiticker.getDimensions()
            wikipediaDayTable.getGranularity() == expectedZonedDayGrain
            wikipediaDayTable.getLogicalMetrics().size() == 7
            wikipediaDayTable.getLogicalMetrics().forEach({ metric -> expectedMetricNames.contains(metric.name) })
            // transitively test on physicalTable content correctness
            wikipediaDayTable.tableGroup.physicalTables.contains(wikiticker)
    }

    def "The air_quality (hour) LogicalTable exists in the loaded Table Dictionary, and contains correct info"() {
        when:
            LogicalTable airQualityHourTable = park.getLogicalTable(new TableIdentifier("air_quality", hour))
            ZonedTimeGrain expectedZonedHourGrain = hour.buildZonedTimeGrain(DateTimeZone.UTC)
            PhysicalTable air = park.getPhysicalTable("air")
            List<String> expectedMetricNames = Arrays.asList("averageCOPerDay", "averageNO2PerDay")
        then:
            airQualityHourTable.getLongName() == "air_quality"
            airQualityHourTable.getCategory() == "GENERAL"
            airQualityHourTable.getDescription() == "air_quality"
            airQualityHourTable.getRetention() == Period.parse("P1Y")
            airQualityHourTable.getDimensions() == air.getDimensions()
            airQualityHourTable.getGranularity() == expectedZonedHourGrain
            airQualityHourTable.getLogicalMetrics().size() == 2
            airQualityHourTable.getLogicalMetrics().forEach({ metric -> expectedMetricNames.contains(metric.name) })
            // transitively test on physicalTable content correctness
            airQualityHourTable.tableGroup.physicalTables.contains(air)
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
