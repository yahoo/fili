// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

class DimensionShowClauseNoneJsonApiDataServletSpec extends BaseDimensionShowClauseDataServletSpec {

    @Override
    String getTarget() {
        return "data/shapes/week/color;show=none"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["width"],
                "dateTime": ["2014-06-02%2F2014-06-09"],
                "format": ["jsonapi"]
        ]
    }

    @Override
    String getExpectedApiResponse() {
        """{
            "color" : [],
            "rows" : [
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "color" : "Color1",
                    "width" : 10
                },
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "color" : "Color2",
                    "width" : 11
                },
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "color" : "Color3",
                    "width" : 12
                }
            ]
        }"""
    }
}
