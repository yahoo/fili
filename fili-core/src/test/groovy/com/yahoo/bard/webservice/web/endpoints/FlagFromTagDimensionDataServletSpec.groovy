// Copyright 2016 Yahoo Inc.
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
import com.yahoo.bard.webservice.web.FlagToTagApiFilterTransformRequestMapperProvider
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
                    new FlagToTagApiFilterTransformRequestMapperProvider().dataApiRequestMapper(resourceDictionaries)
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
                "filters": ["flagFromTag|id-notin[FALSE_VALUE]"],
        ]
    }

    // TODO change the expected responses to what we actually expect

    @Override
    String getExpectedApiResponse() {
        """{
            "rows" : [
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "model|id" : "Model1",
                    "model|desc" : "Model1Desc",
                    "width" : 10
                }
            ]
        }"""
    }

    @Override
    String getExpectedDruidQuery() {
        """{
            "queryType": "groupBy",
            "granularity": ${getTimeGrainString("week")},
            "intervals": [ "2014-06-02T00:00:00.000Z/2014-06-09T00:00:00.000Z" ],
            "dataSource" : {
                "name" : "all_shapes",
                "type" : "table"
            },
            "dimensions": [
                "model"
            ],
            "filter": {
                "fields": [
                    {
                        "dimension": "model",
                        "type": "selector",
                        "value": "Model1"
                    }
                ],
                "type": "or"
            },
            "aggregations": [
                { "name": "width", "fieldName": "width", "type": "longSum" }
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
                    "model" : "Model1",
                    "width" : 10
                }
            }
        ]"""
    }
}
