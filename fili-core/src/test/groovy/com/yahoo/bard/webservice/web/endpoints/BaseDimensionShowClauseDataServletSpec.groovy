// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

abstract class BaseDimensionShowClauseDataServletSpec extends BaseDataServletComponentSpec {

    @Override
    def setup() {
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.findByApiName("color").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Color1", "Color1Desc", "C1BP", "C1RP", "C1GP"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Color2", "Color2Desc", "C2BP", "C2RP", "C2GP"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Color3", "Color3Desc", "C3BP", "C3RP", "C3GP"))
        }
    }

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["width"],
                "dateTime": ["2014-06-02%2F2014-06-09"]
        ]
    }

    @Override
    String getExpectedDruidQuery() {
        """{
            "queryType": "groupBy",
            "granularity": ${getTimeGrainString("week")},
            "intervals": [ "2014-06-02T00:00:00.000Z/2014-06-09T00:00:00.000Z" ],
            "dataSource" : {
                "name" : "color_shapes",
                "type" : "table"
            },
            "dimensions": [
                "color"
            ],
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
                    "color" : "Color1",
                    "width" : 10
                }
            },
            {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                    "color" : "Color2",
                    "width" : 11
                }
            },
            {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                    "color" : "Color3",
                    "width" : 12
                }
            }
        ]"""
    }
}
