// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice.application

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.slurper.webservice.data.config.names.WikiDruidTableName.WIKITICKER

import com.yahoo.bard.webservice.data.config.dimension.TypeAwareDimensionLoader
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService
import com.yahoo.bard.webservice.data.config.ConfigurationLoader
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader
import com.yahoo.luthier.webservice.data.config.dimension.ExternalDimensionsLoader
import com.yahoo.luthier.webservice.data.config.metric.ExternalMetricsLoader
import com.yahoo.luthier.webservice.data.config.table.ExternalTableLoader
import com.yahoo.slurper.webservice.data.config.dimension.DimensionSerializer
import com.yahoo.slurper.webservice.data.config.metric.MetricSerializer
import com.yahoo.slurper.webservice.data.config.table.TableSerializer
import com.yahoo.slurper.webservice.data.config.auto.StaticWikiConfigLoader
import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class ConfigurationLoadTaskSpec extends Specification {

    @Shared ConfigurationLoader loader
    @Shared DimensionDictionary dimensionDictionary
    @Shared MetricDictionary metricDictionary
    @Shared LogicalTableDictionary logicalTableDictionary
    @Shared PhysicalTableDictionary physicalTableDictionary

    def setupSpec() {
        StaticWikiConfigLoader wikiConfigLoader = new StaticWikiConfigLoader()

        final EXTERNAL_CONFIG_FILE_PATH  = "src/test/resources/"
        final ExternalConfigLoader externalConfigLoader = new ExternalConfigLoader()

        DimensionSerializer dimensionSerializer = new DimensionSerializer(new ObjectMapper());

        dimensionSerializer
                .setConfig(wikiConfigLoader)
                .setPath( EXTERNAL_CONFIG_FILE_PATH)
                .parseToJson()

        MetricSerializer metricSerializer = new MetricSerializer(new ObjectMapper());

        metricSerializer
                .setConfig(wikiConfigLoader)
                .setPath(EXTERNAL_CONFIG_FILE_PATH)
                .parseToJson()

        TableSerializer TableSerializer = new TableSerializer(new ObjectMapper());

        TableSerializer
                .setDimensions(dimensionSerializer)
                .setMetrics(metricSerializer)
                .setConfig(wikiConfigLoader)
                .setPath(EXTERNAL_CONFIG_FILE_PATH)
                .parseToJson()

        ExternalDimensionsLoader externalDimensionsLoader = new ExternalDimensionsLoader(
                externalConfigLoader,
                EXTERNAL_CONFIG_FILE_PATH
        )

        LinkedHashSet<DimensionConfig> dimensions = externalDimensionsLoader.getAllDimensionConfigurations()

        ExternalTableLoader tablesLoader = new ExternalTableLoader(
                new TestDataSourceMetadataService(),
                externalDimensionsLoader,
                externalConfigLoader,
                EXTERNAL_CONFIG_FILE_PATH
        )

        loader = new ConfigurationLoader(
                new TypeAwareDimensionLoader(dimensions),
                new ExternalMetricsLoader(
                        externalConfigLoader,
                        EXTERNAL_CONFIG_FILE_PATH
                ),
                tablesLoader
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
        tableIdName                       | tableIdGrain | logicalTableName
        WIKITICKER.asName().toLowerCase() | HOUR         | WIKITICKER.asName().toLowerCase()
    }

    def "test fetching of physicalTable by its name"() {
        expect: "fetched table has the same name as that requested"
        physicalTableDictionary.get(WIKITICKER.asName()).getName() == WIKITICKER.asName()
    }
}
