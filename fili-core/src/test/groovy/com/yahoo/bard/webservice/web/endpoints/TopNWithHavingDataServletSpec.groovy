// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import static com.yahoo.bard.webservice.config.BardFeatureFlag.TOP_N

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

import spock.lang.Shared

/**
 * When we execute a TopN that meets the criteria for a Druid TopN, but has a having clause, then we send a groupBy
 * query to Druid, not a TopN query.
 */
class TopNWithHavingDataServletSpec extends BaseDataServletComponentSpec {

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
                "metrics": ["width", "height"],
                "dateTime": ["2014-06-02%2F2014-06-04"],
                "sort": ["width|DESC"],
                "topN": ["2"],
                "having": ["width-gt[10]", "width-lt[12]", "height-eq[50]"]
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
                    "height": 50,
                    "width": 11
                },
                {
                    "dateTime": "2014-06-02 00:00:00.000",
                    "model|id": "Model3",
                    "model|desc": "Model3Desc",
                    "height": 50,
                    "width": 11
                },
                {
                    "dateTime": "2014-06-03 00:00:00.000",
                    "model|id": "Model3",
                    "model|desc": "Model3Desc",
                    "height": 50,
                    "width": 11
                },
                {
                    "dateTime": "2014-06-03 00:00:00.000",
                    "model|id": "Model2",
                    "model|desc": "Model2Desc",
                    "height": 50,
                    "width": 11
                }
            ]
        }"""
    }

    @Override
    String getExpectedDruidQuery() {
        """{
                "queryType": "groupBy",
                "granularity": ${getTimeGrainString()},
                "intervals": [ "2014-06-02T00:00:00.000Z/2014-06-04T00:00:00.000Z" ],
                "dataSource": {
                    "name": "all_shapes",
                    "type": "table"
                },
                "dimensions": ["model"],
                "aggregations": [
                    { "name": "width", "fieldName": "width", "type": "longSum" },
                    { "name": "height", "fieldName": "height", "type": "longSum" }
                ],
                "postAggregations": [],
                "having": {
                    "havingSpecs": [
                        {
                            "aggregation": "height",
                            "type": "equalTo",
                            "value": 50
                        },
                        {
                            "havingSpecs": [
                                {
                                    "aggregation": "width",
                                    "type": "greaterThan",
                                    "value": 10
                                },
                                {
                                    "aggregation": "width",
                                    "type": "lessThan",
                                    "value": 12
                                }
                            ],
                            "type": "and"
                        }
                    ],
                    "type": "and"
                },
                "limitSpec": {
                    "type": "default",
                    "columns": [
                        {
                            "dimension": "width",
                            "direction": "DESC"
                        }
                    ]
                },
                "context": {}
        }"""
    }

    @Override
    String getFakeDruidResponse() {
        """[
            {
                "version": "v1",
                "timestamp": "2014-06-02T00:00:00.000Z",
                "event": {
                    "model": "Model1",
                    "width": 11,
                    "height": 50
                }
            },
            {
                "version": "v1",
                "timestamp": "2014-06-02T00:00:00.000Z",
                "event": {
                    "model": "Model3",
                    "width": 11,
                    "height": 50
                }
            },
            {
                "version": "v1",
                "timestamp": "2014-06-02T00:00:00.000Z",
                "event": {
                    "model": "Model2",
                    "width": 11,
                    "height": 50
                }
            },
            {
                "version": "v1",
                "timestamp": "2014-06-03T00:00:00.000Z",
                "event": {
                    "model": "Model3",
                    "width": 11,
                    "height": 50
                }
            },
            {
                "version": "v1",
                "timestamp": "2014-06-03T00:00:00.000Z",
                "event": {
                    "model": "Model2",
                    "width": 11,
                    "height": 50
                }
            },
            {
                "version": "v1",
                "timestamp": "2014-06-03T00:00:00.000Z",
                "event": {
                    "model": "Model1",
                    "width": 11,
                    "height": 50
                }
            }
        ]"""
    }
}
