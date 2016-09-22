// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import static com.yahoo.bard.webservice.config.BardFeatureFlag.TOP_N

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

import spock.lang.Shared

/**
 * This spec tests the format for an optimized (druid native) topN data request that corresponds to ascending order.
 */
class TopNAscendingDataServletSpec extends BaseDataServletComponentSpec {

    @Shared boolean topNStatus

    def setupSpec() {
        topNStatus = TOP_N.isOn();
        TOP_N.setOn(true)
    }

    def cleanupSpec() {
        TOP_N.setOn(topNStatus)
    }

    @Override
    def setup() {
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.findByApiName("model").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Model1", "Model1Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Model2", "Model2Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Model3", "Model3Desc"))
        }
    }

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "data/shapes/day/model"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics": ["width"],
                "dateTime": ["2014-06-02%2F2014-06-04"],
                "sort": ["width|ASC"],
                "topN": ["2"]
        ]
    }

    @Override
    String getExpectedApiResponse() {
        """{
            "rows": [
                {
                    "dateTime": "2014-06-02 00:00:00.000",
                    "model|id": "Model1",
                    "model|desc": "Model1Desc",
                    "width": 8
                },
                {
                    "dateTime": "2014-06-02 00:00:00.000",
                    "model|id": "Model3",
                    "model|desc": "Model3Desc",
                    "width": 9
                },
                {
                    "dateTime": "2014-06-03 00:00:00.000",
                    "model|id": "Model3",
                    "model|desc": "Model3Desc",
                    "width": 6
                },
                {
                    "dateTime": "2014-06-03 00:00:00.000",
                    "model|id": "Model2",
                    "model|desc": "Model2Desc",
                    "width": 7
                }
            ]
        }"""
    }

    @Override
    String getExpectedDruidQuery() {
        """{
            "queryType": "topN",
            "granularity": ${getTimeGrainString()},
            "intervals": [ "2014-06-02T00:00:00.000Z/2014-06-04T00:00:00.000Z" ],
            "dataSource": {
                "name": "all_shapes",
                "type": "table"
            },
            "dimension": "model",
            "threshold": 2,
            "metric": {
                "type": "inverted",
                "metric": {
                    "type": "numeric",
                    "metric": "width"
                }
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
                "timestamp": "2014-06-02T00:00:00.000Z",
                "result": [
                    {
                        "model": "Model1",
                        "width": 8
                    },
                    {
                        "model": "Model3",
                        "width": 9
                    }
                ]
            },
            {
                "timestamp": "2014-06-03T00:00:00.000Z",
                "result": [
                    {
                        "model": "Model3",
                        "width": 6
                    },
                    {
                        "model": "Model2",
                        "width": 7
                    }
                ]
            }
        ]"""
    }
}
