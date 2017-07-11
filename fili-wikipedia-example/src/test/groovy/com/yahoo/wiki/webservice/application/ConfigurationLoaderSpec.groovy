// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.wiki.webservice.data.config.names.WikiLogicalTableName.WIKIPEDIA
import static com.yahoo.wiki.webservice.data.config.names.WikiDruidTableName.WIKITICKER

import com.yahoo.bard.webservice.data.config.ConfigurationLoader
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.config.dimension.TypeAwareDimensionLoader
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.wiki.webservice.data.config.dimension.WikiDimensions
import com.yahoo.wiki.webservice.data.config.metric.WikiMetricLoader
import com.yahoo.wiki.webservice.data.config.table.WikiTableLoader

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class ConfigurationLoaderSpec extends Specification {

    @Shared ConfigurationLoader loader
    @Shared DimensionDictionary dimensionDictionary
    @Shared MetricDictionary metricDictionary
    @Shared LogicalTableDictionary logicalTableDictionary
    @Shared PhysicalTableDictionary physicalTableDictionary

    def setupSpec() {
        LinkedHashSet<DimensionConfig> dimensions = new WikiDimensions().getAllDimensionConfigurations();
        loader = new ConfigurationLoader(
                new TypeAwareDimensionLoader(dimensions),
                new WikiMetricLoader(),
                new WikiTableLoader()
        )
        loader.load()

        dimensionDictionary = loader.getDimensionDictionary()
        metricDictionary = loader.getMetricDictionary()
        logicalTableDictionary = loader.getLogicalTableDictionary()
        physicalTableDictionary = loader.getPhysicalTableDictionary()
    }

    @Unroll
    def "test Dimension dictionary #dim"() {
        expect:
        dimensionDictionary.findByApiName(dim.apiName)?.apiName == dim.apiName

        where:
        dim << dimensionDictionary.findAll()
    }

    def "test table keys"() {
        setup:
        TableIdentifier ti1 = new TableIdentifier("table", DAY)
        TableIdentifier ti2 = new TableIdentifier("table", DAY)
        TableIdentifier ti3 = new TableIdentifier("table", HOUR)

        expect:
        ti1 == ti2
        ti1.hashCode() == ti2.hashCode()
        ti1 != ti3
    }

    @Unroll
    def "logical table #logicalTableName with granularity #tableIdGrain is successfully loaded into the dictionary"() {
        expect:
        logicalTableDictionary.get(new TableIdentifier(tableIdName, tableIdGrain)).getName() ==
                logicalTableName

        where:
        tableIdName                      | tableIdGrain | logicalTableName
        WIKIPEDIA.asName().toLowerCase() | HOUR         | WIKIPEDIA.asName().toLowerCase()
    }

    def "test fetching of physicalTable by its name"() {
        expect: "fetched table has the same name as that requested"
        physicalTableDictionary.get(WIKITICKER.asName()).getName() == WIKITICKER.asName()
    }
}
