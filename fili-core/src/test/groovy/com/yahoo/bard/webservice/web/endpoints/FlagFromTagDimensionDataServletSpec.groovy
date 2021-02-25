// Copyright 2019 Oath Inc.
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
import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableSchema
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.web.DataApiRequestMapperUtils
import com.yahoo.bard.webservice.web.FilterOptimizingRequestMapper
import com.yahoo.bard.webservice.web.RequestMapper
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest

import java.util.stream.Collectors

/**
 * TODO the setup can probably be moved into table test resources instead.
 */
class FlagFromTagDimensionDataServletSpec extends BaseDataServletComponentSpec {

//     We need to inject the FlagFromTagRequestMapperProvider into the set of data request mappers
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
                        new FilterOptimizingRequestMapper(resourceDictionaries, DataApiRequestMapperUtils.identityMapper(resourceDictionaries))
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
        // setup flag from tag dimension and its dependencies.
        Dimension filteringDimension = Mock()
        filteringDimension.getApiName() >> "filteringDimension"
        filteringDimension.getKey() >> DefaultDimensionField.ID
        filteringDimension.getDimensionFields() >> { [DefaultDimensionField.ID] as LinkedHashSet }
        filteringDimension.isAggregatable() >> true

        SearchProvider filteringSP = new NoOpSearchProvider(100)
        filteringSP.setDimension(filteringDimension)

        filteringDimension.getSearchProvider() >> filteringSP

        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.add(filteringDimension)

        FlagFromTagDimensionConfig.Builder builder = new FlagFromTagDimensionConfig.Builder(
                { "flagFromTag" },
                "breed", // grouping dimension physical name
                "fftDescription",
                "fftLongName",
                "fftCategory",
                "filteringDimension", // filtering
                "TAG_VALUE"
        )

        ((LookupDimension) dimensionStore.findByApiName("breed")).getExtractionFunction().ifPresent({it -> builder.addExtractionFunction(it)})
        FlagFromTagDimensionConfig fftConfig = builder.trueValue("TRUE_VALUE").falseValue("FALSE_VALUE").build()

        fft = new FlagFromTagDimension(fftConfig, dimensionStore)
        dimensionStore.add(fft)

        // Flag from tag dimension needs to be on the logical table and relevant physical tables
        LogicalTableDictionary logicalDictionary = jtb.configurationLoader.logicalTableDictionary
        LogicalTable table = logicalDictionary.get(
                new TableIdentifier(TestLogicalTableName.PETS.asName(), DefaultTimeGrain.DAY)
        )
        DimensionColumn fftColumn = new DimensionColumn(fft)
        DimensionColumn filterDimColumn = new DimensionColumn(filteringDimension)

        table.schema.columns.add(fftColumn)
        table.schema.columns.add(filterDimColumn)

        // generate a new schema per physical table including FFT and filtering dimension.
        Set<PhysicalTable> newTables = table.tableGroup.getPhysicalTables().stream()
                .map({
                    it ->
                        ZonedTimeGrain grain = it.getSchema().getTimeGrain()

                        Set<Column> columns = new HashSet<>(it.getSchema().getColumns())
                        columns.add(fftColumn)
                        columns.add(filterDimColumn)

                        Map<String, String> logicalToPhysicalMapping = new HashMap<>(it.getSchema().logicalToPhysicalColumnNames)
                        logicalToPhysicalMapping.put("filteringDimension", "filteringDimension")
                        logicalToPhysicalMapping.put("flagFromTag", "breed")

                        PhysicalTable newTable = Spy(it)
                        newTable.getSchema() >> new PhysicalTableSchema(grain, columns, logicalToPhysicalMapping)
                        return newTable
                })
                .collect(Collectors.toSet())

        // finally remove all the old tables and add all the spys.
        table.tableGroup.tables.clear()
        newTables.each { it -> table.tableGroup.tables.add(it)}
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
                          "expr": "^(.+,)*(TAG_VALUE)(,.+)*\$",
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
