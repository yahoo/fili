// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import static com.yahoo.bard.webservice.config.BardFeatureFlag.TOP_N

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

import spock.lang.Shared

/**
 * This spec tests the default format for a topN data request with multiple dimensions that corresponds to descending
 * order.
 */
class TopNMultipleDimensionsDataServletSpec extends BaseDataServletComponentSpec {

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
        dimensionStore.findByApiName("color").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "color1", "color1Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "color2", "color2Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "color3", "color3Desc"))
        }
    }

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "data/shapes/day/model/color"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["width"],
                "dateTime": ["2014-06-02%2F2014-06-04"],
                "sort"    : ["width|DESC"],
                "topN"    : ["2"]
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
                    "color|id": "color1",
                    "color|desc": "color1Desc",
                    "width": 10
                },
                {
                    "dateTime": "2014-06-02 00:00:00.000",
                    "model|id": "Model3",
                    "model|desc": "Model3Desc",
                    "color|id": "color3",
                    "color|desc": "color3Desc",
                    "width": 9
                },
                {
                    "dateTime": "2014-06-03 00:00:00.000",
                    "model|id": "Model3",
                    "model|desc": "Model3Desc",
                    "color|id": "color3",
                    "color|desc": "color3Desc",
                    "width": 8
                },
                {
                    "dateTime": "2014-06-03 00:00:00.000",
                    "model|id": "Model2",
                    "model|desc": "Model2Desc",
                    "color|id": "color2",
                    "color|desc": "color2Desc",
                    "width": 7
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
                "dimensions": ["model", "color"],
                "aggregations": [
                    { "name": "width", "fieldName": "width", "type": "longSum" }
                ],
                "postAggregations": [],
                "limitSpec": {
                    "type": "default",
                    "columns": [
                        {
                            "dimension": "width",
                            "direction": "DESC"
                        }
                    ]
                },
                "context":{}
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
                    "color": "color1",
                    "width": 10
                }
            },
            {
                "version": "v1",
                "timestamp": "2014-06-02T00:00:00.000Z",
                "event": {
                    "model": "Model3",
                    "color": "color3",
                    "width": 9
                }
            },
            {
                "version": "v1",
                "timestamp": "2014-06-02T00:00:00.000Z",
                "event": {
                    "model": "Model2",
                    "color": "color2",
                    "width": 8
                }
            },
            {
                "version": "v1",
                "timestamp": "2014-06-03T00:00:00.000Z",
                "event": {
                    "model": "Model3",
                    "color": "color3",
                    "width": 8
                }
            },
            {
                "version": "v1",
                "timestamp": "2014-06-03T00:00:00.000Z",
                "event": {
                    "model": "Model2",
                    "color": "color2",
                    "width": 7
                }
            },
            {
                "version": "v1",
                "timestamp": "2014-06-03T00:00:00.000Z",
                "event": {
                    "model": "Model1",
                    "color": "color1",
                    "width": 6
                }
            }
        ]"""
    }
}
