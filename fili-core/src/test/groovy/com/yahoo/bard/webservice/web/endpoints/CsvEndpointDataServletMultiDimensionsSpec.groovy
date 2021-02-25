// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

import javax.ws.rs.core.MultivaluedMap

/**
 * A test that verifies that the ordering of CSV columns is consistent with the ordering of the requested dimensions
 * and metrics. Dimensions should come before metrics, both in the order they were made in the request.
 */
class CsvEndpointDataServletMultiDimensionsSpec extends BaseDataServletComponentSpec {

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
        headers.getFirst("Content-Disposition") ==
                "attachment; filename=data-shapes-week-color-shape-size_2014-06-02_2014-06-09.csv" &&
        headers.getFirst("Content-Type") == "text/csv; charset=utf-8"
    }

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "/data/shapes/week/color/shape/size"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["width", "height"],
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
                "name" : "color_size_shape_shapes",
                "type" : "table"
            },
            "dimensions": ["color","size","shape"],
            "aggregations": [
                {
                    "name": "width",
                    "fieldName": "width",
                    "type": "longSum"
                },
                {
                    "name": "height",
                    "fieldName": "height",
                    "type": "longSum"
                }
            ],
            "postAggregations": [],
            "context": {}
        }"""
    }

    @Override
    String getExpectedApiResponse() {
        return 'dateTime,color|id,color|desc,shape|id,shape|desc,size|id,size|desc,width,height\n' +
        '"2014-06-02 00:00:00.000",Color1,Color1Desc,Shape1,,Size1,,10,13\n' +
        '"2014-06-02 00:00:00.000",Color2,Color2Desc,Shape2,,Size2,,11,14\n' +
        '"2014-06-02 00:00:00.000",Color3,Color3Desc,Shape3,,Size3,,12,15\n'
    }

    @Override
    String getFakeDruidResponse() {
        """[
            {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                    "color" : "Color1",
                    "shape": "Shape1",
                    "size": "Size1",
                    "width" : 10,
                    "height": 13
                }
            },
            {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                    "color" : "Color2",
                    "shape" : "Shape2",
                    "size" : "Size2",
                    "width" : 11,
                    "height": 14
                }
            },
            {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                    "color" : "Color3",
                    "shape": "Shape3",
                    "size": "Size3",
                    "width" : 12,
                    "height": 15
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
