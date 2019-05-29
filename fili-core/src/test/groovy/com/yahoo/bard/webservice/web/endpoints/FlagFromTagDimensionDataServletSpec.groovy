// Copyright 2019 Verizon Media Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.ApplicationState
import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.application.TestBinderFactory
import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.config.dimension.FlagFromTagDimensionConfig
import com.yahoo.bard.webservice.data.config.metric.MetricLoader
import com.yahoo.bard.webservice.data.config.names.TestLogicalTableName
import com.yahoo.bard.webservice.data.config.table.TableLoader
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.data.dimension.impl.FlagFromTagDimension
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.web.FlagFromTagRequestMapperProvider
import com.yahoo.bard.webservice.web.RequestMapper
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest

class FlagFromTagDimensionDataServletSpec extends BaseDataServletComponentSpec {

    // Test classes to inject mappers into binder factory.
    class TestResultMapperBinderFactory extends TestBinderFactory {

        TestResultMapperBinderFactory(
                LinkedHashSet<DimensionConfig> dimensionConfig,
                MetricLoader metricLoader,
                TableLoader tableLoader,
                ApplicationState state
        ) {
            super(dimensionConfig, metricLoader, tableLoader, state)
        }

        @Override
        Map<String, RequestMapper> getRequestMappers(ResourceDictionaries resourceDictionaries) {
            Map<String, RequestMapper> mappers = [:] as Map
            mappers.put(
                    DataApiRequest.REQUEST_MAPPER_NAMESPACE,
                    new FlagFromTagRequestMapperProvider.Builder().build().dataMapper(resourceDictionaries)
            )
            return mappers
        }
    }

    class TestResultMapperBinder extends JerseyTestBinder {

        TestResultMapperBinder(final Class<?>... resourceClasses) {
            super(resourceClasses)
        }

        @Override
        TestBinderFactory buildBinderFactory(
                LinkedHashSet<DimensionConfig> dimensionConfiguration,
                MetricLoader metricLoader,
                TableLoader tableLoader,
                ApplicationState state
        ) {
            return new TestResultMapperBinderFactory(dimensionConfiguration, metricLoader, tableLoader, state)
        }
    }

    FlagFromTagDimension fft

    @Override
    def setup() {
        Dimension filteringDimension = Mock()
        filteringDimension.getApiName() >> "filteringDimension"
        filteringDimension.getKey() >> DefaultDimensionField.ID
        filteringDimension.getDimensionFields() >> { [DefaultDimensionField.ID] as LinkedHashSet }

        SearchProvider filteringSP = new NoOpSearchProvider(100)
        filteringSP.setDimension(filteringDimension)

        filteringDimension.getSearchProvider() >> filteringSP


        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.add(filteringDimension)

        FlagFromTagDimensionConfig fftConfig = new FlagFromTagDimensionConfig(
                {"flagFromTag"},
                "fftDescription",
                "fftLongName",
                "fftCategory",
                "filteringDimension", // filtering
                "breed", // grouping, should already be in dimension dictionary
                "TAG_VALUE",
                "TRUE_VALUE",
                "FALSE_VALUE",
        )
        fft = new FlagFromTagDimension(fftConfig, dimensionStore)
        dimensionStore.add(fft)

        LogicalTableDictionary logicalDictionary = jtb.configurationLoader.logicalTableDictionary
        LogicalTable table = logicalDictionary.get(new TableIdentifier(TestLogicalTableName.PETS.asName(), DefaultTimeGrain.DAY))
        DimensionColumn fftColumn = new DimensionColumn(fft)
        DimensionColumn filterDimColumn = new DimensionColumn(filteringDimension)
        table.schema.columns.add(fftColumn)
        table.schema.columns.add(filterDimColumn)
        table.tableGroup.getPhysicalTables().each { it -> it.schema.columns.add(fftColumn)}
        table.tableGroup.getPhysicalTables().each { it -> it.schema.columns.add(filterDimColumn)}
    }

    @Override
    JerseyTestBinder buildTestBinder() {
        new TestResultMapperBinder(resourceClasses)
    }

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "data/pets/day/flagFromTag"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics": ["limbs"],
                "dateTime": ["2014-06-02%2F2014-06-09"],
                "filters": ["flagFromTag|id-in[FALSE_VALUE]"],
        ]
    }

    // TODO change the expected responses to what we actually expect

    @Override
    String getExpectedApiResponse() {
        """{
            "rows" : [
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "flagFromTag|id" : "FALSE_VALUE",
                    "limbs" : 4
                }
            ]
        }"""
    }

    @Override
    String getExpectedDruidQuery() {
        """{
            "queryType": "groupBy",
            "granularity": ${getTimeGrainString("day")},
            "intervals": [ "2014-06-02T00:00:00.000Z/2014-06-09T00:00:00.000Z" ],
            "dataSource" : {
                "name" : "all_pets",
                "type" : "table"
            },
            "dimensions": [
               {
                  "dimension":"breed",
                  "outputName":"flagFromTag",
                  "type": "extraction",
                  "extractionFn": {
                      "type": "cascade",
                      "extractionFns": [
                        {
                          "type": "lookup",
                          "lookup": {
                            "type": "namespace",
                            "namespace": "NAMESPACE1"
                          },
                          "retainMissingValue": false,
                          "replaceMissingValueWith": "Unknown NAMESPACE1",
                          "injective": false,
                          "optimize": true
                        },
                        {
                          "type": "lookup",
                          "lookup": {
                            "type": "namespace",
                            "namespace": "NAMESPACE2"
                          },
                          "retainMissingValue": false,
                          "replaceMissingValueWith": "Unknown NAMESPACE2",
                          "injective": false,
                          "optimize": true
                        },
                        {
                          "type": "regex",
                          "expr": "(.+,)*(TAG_VALUE)(,.+)*",
                          "index": 2,
                          "replaceMissingValueWith": "",
                          "replaceMissingValue": true
                        },
                        {
                          "type": "lookup",
                          "lookup": {
                            "type": "map",
                            "map": {
                              "TAG_VALUE": "TRUE_VALUE"
                            }
                          },
                          "retainMissingValue": false,
                          "replaceMissingValueWith": "FALSE_VALUE",
                          "injective": false,
                          "optimize": false
                        }
                      ]
                  }
               }
            ],
            "filter": {
                "type": "not",
                "field" : {
                    "fields": [
                        {
                            "dimension": "filteringDimension",
                            "type": "selector",
                            "value": "TAG_VALUE"
                        }
                    ],
                    "type": "or"
                }
            },
            "aggregations": [
                { "name": "limbs", "fieldName": "limbs", "type": "longSum" }
            ],
            "postAggregations": [],
            "context": {}
        }"""
    }

    @Override
    String getFakeDruidResponse() {
        """[
            {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                    "flagFromTag" : "FALSE_VALUE",
                    "limbs" : 4
                }
            }
        ]"""
    }
}
