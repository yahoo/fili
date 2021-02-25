// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

import javax.ws.rs.core.MultivaluedMap

class CsvEndpointDataServletSpec extends BaseDataServletComponentSpec {

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
    boolean headersAreCorrect(MultivaluedMap<String, Object> headers){
        headers.getFirst("Content-Disposition") == "attachment; filename=data-shapes-week-color_2014-06-02_2014-06-09.csv" &&
        headers.getFirst("Content-Type") == "text/csv; charset=utf-8"
    }

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "/data/shapes/week/color"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["width"],
                "dateTime": ["2014-06-02%2F2014-06-09"],
                "format": ["CSV"]
        ]
    }

    @Override
    String getExpectedDruidQuery() {
        """{
            "queryType": "groupBy",
            "granularity" : {"type":"period","period":"P1W","timeZone":"UTC"},
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
    String getExpectedApiResponse() {
        return 'dateTime,color|id,color|desc,width\n' +
        '"2014-06-02 00:00:00.000",Color1,Color1Desc,10\n' +
        '"2014-06-02 00:00:00.000",Color2,Color2Desc,11\n' +
        '"2014-06-02 00:00:00.000",Color3,Color3Desc,12\n'
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

    @Override
    boolean validateExpectedApiResponse(String expectedApiResponse) {
        true
    }

    @Override
    boolean compareResult(String result, String expectedResult) {
        result == expectedResult
    }
}
