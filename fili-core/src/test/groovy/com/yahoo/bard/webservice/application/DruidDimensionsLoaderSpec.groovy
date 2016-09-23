// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProviderManager
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.druid.client.SuccessCallback
import com.yahoo.bard.webservice.table.PhysicalTableDictionary

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.joda.JodaModule

import org.joda.time.DateTime

import spock.lang.Specification

class DruidDimensionsLoaderSpec extends Specification {

    static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JodaModule())
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    static final List<String> LOADED_DIMENSIONS = [
            TestApiDimensionName.SHAPE,
            TestApiDimensionName.SIZE,
            TestApiDimensionName.COLOR
    ]*.asName()


    SystemConfig systemConfig = SystemConfigProvider.getInstance()

    DruidDimensionsLoader loader
    String druidDimLoaderDimensions
    DruidWebService druidWebService
    DimensionDictionary dimensionDictionary
    JerseyTestBinder jtb

    def setup() {
        SystemConfig systemConfig = SystemConfigProvider.getInstance()
        druidDimLoaderDimensions = systemConfig.getStringProperty(
                DruidDimensionsLoader.DRUID_DIM_LOADER_DIMENSIONS,
                null
        )
        systemConfig.setProperty(
                DruidDimensionsLoader.DRUID_DIM_LOADER_DIMENSIONS,
                LOADED_DIMENSIONS.join(',')
        )

        jtb = new JerseyTestBinder()
        jtb.buildWebServices()
        PhysicalTableDictionary physicalTables = jtb.configurationLoader.physicalTableDictionary
        dimensionDictionary = jtb.getConfigurationLoader().dimensionDictionary
        druidWebService = Mock(DruidWebService)
        loader = new DruidDimensionsLoader(physicalTables, dimensionDictionary, druidWebService)
    }

    def cleanup() {
        jtb.tearDown()
        if (druidDimLoaderDimensions == null) {
            systemConfig.clearProperty(DruidDimensionsLoader.DRUID_DIM_LOADER_DIMENSIONS)
        } else {
            systemConfig.setProperty(DruidDimensionsLoader.DRUID_DIM_LOADER_DIMENSIONS, druidDimLoaderDimensions)
        }
    }

    def "The DimensionLoader constructor successfully extracts the dimensions from a dimension dictionary"() {
        expect: "A list of singleton dimension lists that need to be loaded from Druid"
        loader.dimensions == LOADED_DIMENSIONS.collect {[dimensionDictionary.findByApiName(it)]}
    }

    def "When run, the DruidDimensionLoader sends the correct number of Druid queries"() {
        given: "A list of resolved dimensions that should be loaded by the loader"
        List<Dimension> dimensions = LOADED_DIMENSIONS.collect {dimensionDictionary.findByApiName(it)}

        and: "The number of expected queries to Druid"
        int numDruidQueries = dimensions.size() * jtb.configurationLoader.physicalTableDictionary.size()

        when:
        loader.run()

        then: "A query is sent to Druid for each dimension and each data store"
        numDruidQueries * druidWebService.postDruidQuery(_, _, _, _, _)
    }

    def "The success callback correctly loads JSON dimension data into the dimension"() {
        given: "A dimension to load"
        Dimension dimension = new KeyValueStoreDimension(
                "gender",
                "gender",
                [BardDimensionField.ID] as LinkedHashSet,
                MapStoreManager.getInstance("gender"),
                NoOpSearchProviderManager.getInstance("gender")
        )

        and: "The callback to test"
        SuccessCallback callback = loader.buildDruidDimensionsSuccessCallback(dimension)

        and: "The data to load with the callback"
        String jsonResult = """[
                {
                    "timestamp": "2012-01-01T00:00:00.000Z",
                    "result": [
                        {
                           "dimension": "gender",
                           "value": "male"
                        }, {
                            "dimension": "gender",
                            "value": "female"
                        }, {
                            "dimension": "gender",
                            "value": "unknown"
                        }
                    ]
                }
            ]"""
        JsonNode node = MAPPER.readTree(jsonResult)

        when: "We invoke the callback on the data"
        callback.invoke(node)

        then: "The dimension rows have been successfully loaded into the dimension"
        dimension.findDimensionRowByKeyValue("male")?.getRowMap()["id"] == "male"
        dimension.findDimensionRowByKeyValue("female")?.getRowMap()["id"] == "female"
        dimension.findDimensionRowByKeyValue("unknown")?.getRowMap()["id"] == "unknown"
    }

    def "The success callback doesn't fail when it tries to load already existing dimension value"() {
        given: "A dimension to load"
        Dimension dimension = new KeyValueStoreDimension(
                "gender",
                "gender",
                [BardDimensionField.ID] as LinkedHashSet,
                MapStoreManager.getInstance("gender"),
                NoOpSearchProviderManager.getInstance("gender")
        )

        and: "A dimension value to load"
        String dimensionValueToLoad = """[
            {
                "timestamp": "2012-01-01T00:00:00.000Z",
                "result": [
                    {
                       "dimension": "gender",
                       "value": "male"
                    }
                ]
            }
        ]"""

        and: "The callback to test"
        SuccessCallback callback = loader.buildDruidDimensionsSuccessCallback(dimension)

        and: "The dimension value is already loaded once"
        if (dimension.findDimensionRowByKeyValue("male") == null) {
            JsonNode node = MAPPER.readTree(dimensionValueToLoad)
            callback.invoke(node)
        }

        and: "Try to load dimension value which is already loaded"
        JsonNode node = MAPPER.readTree(dimensionValueToLoad)

        when: "We invoke the callback on the data"
        callback.invoke(node)

        then: "The dimension row value was ignored as it was already loaded"
        dimension.findDimensionRowByKeyValue("male")?.getRowMap()["id"] == "male"
    }

    def "The success callback correctly sets the lastUpdated date on the dimension, even if there is no data"() {
        given: "A dimension to load"
        Dimension dimension = new KeyValueStoreDimension(
                "gender",
                "gender",
                [BardDimensionField.ID] as LinkedHashSet,
                MapStoreManager.getInstance("gender"),
                NoOpSearchProviderManager.getInstance("gender")
        )
        DateTime previousLastUpdated = dimension.lastUpdated

        and: "The callback to test"
        SuccessCallback callback = loader.buildDruidDimensionsSuccessCallback(dimension)

        and: "The data to load with the callback"
        String jsonResult = "[]"
        JsonNode node = MAPPER.readTree(jsonResult)

        when: "We invoke the callback on the data"
        callback.invoke(node)

        then: "The dimension's lastUpdated date has been updated"
        dimension.lastUpdated != previousLastUpdated
    }
}
