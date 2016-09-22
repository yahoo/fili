// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

class SingleDimensionNoRowsFilterDataServletSpec extends BaseDataServletComponentSpec {
    @Override
    def setup() {
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.findByApiName("model").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Model1", "Model1Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Model12", "Model12Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Model123", "Model123Desc"))
        }
    }

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "data/shapes/week/model"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["width"],
                "dateTime": ["2014-06-02%2F2014-06-09"],
                "filters" : ["other|id-in[other1]", "other|desc-in[other2Desc]"]
        ]
    }

    @Override
    String getExpectedApiResponse() {
        """{
            "rows" : [
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "model|id" : "Model1",
                    "model|desc" : "Model1Desc",
                    "width" : 10
                },
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "model|id" : "Model12",
                    "model|desc" : "Model12Desc",
                    "width" : 11
                },
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "model|id" : "Model123",
                    "model|desc" : "Model123Desc",
                    "width" : 12
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
                        "fields": [
                            {
                                "dimension": "misc",
                                "type": "selector",
                                "value": "other1"
                            }
                        ],
                        "type": "or"
                    },
                    {
                        "fields": [
                            {
                                "dimension": "misc",
                                "type": "selector",
                                "value": "other2Desc"
                            }
                        ],
                        "type": "or"
                    }
                ],
                "type": "and"
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
            },
            {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                    "model" : "Model12",
                    "width" : 11
                }
            },
            {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                    "model" : "Model123",
                    "width" : 12
                }
            }
        ]"""
    }
}
