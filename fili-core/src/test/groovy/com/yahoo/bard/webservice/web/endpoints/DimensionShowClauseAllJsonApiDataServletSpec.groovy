// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

class DimensionShowClauseAllJsonApiDataServletSpec extends BaseDimensionShowClauseDataServletSpec {

    @Override
    String getTarget() {
        return "data/shapes/week/color;show=all"
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
            "color" : [
                {
                    "id": "Color1",
                    "desc": "Color1Desc",
                    "bluePigment": "C1BP",
                    "redPigment": "C1RP",
                    "greenPigment": "C1GP"
                },
                {
                    "id": "Color2",
                    "desc": "Color2Desc",
                    "bluePigment": "C2BP",
                    "redPigment": "C2RP",
                    "greenPigment": "C2GP"
                },
                {
                    "id": "Color3",
                    "desc": "Color3Desc",
                    "bluePigment": "C3BP",
                    "redPigment": "C3RP",
                    "greenPigment": "C3GP"
                }
            ],
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
