// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

class DimensionShowClauseAllDataServletSpec extends BaseDimensionShowClauseDataServletSpec {

    @Override
    String getTarget() {
        return "data/shapes/week/color;show=all"
    }

    @Override
    String getExpectedApiResponse() {
        """{
            "rows" : [
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "color|id" : "Color1",
                    "color|desc" : "Color1Desc",
                    "color|bluePigment" : "C1BP",
                    "color|redPigment" : "C1RP",
                    "color|greenPigment" : "C1GP",
                    "width" : 10
                },
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "color|id" : "Color2",
                    "color|desc" : "Color2Desc",
                    "color|bluePigment" : "C2BP",
                    "color|redPigment" : "C2RP",
                    "color|greenPigment" : "C2GP",
                    "width" : 11
                },
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "color|id" : "Color3",
                    "color|desc" : "Color3Desc",
                    "color|bluePigment" : "C3BP",
                    "color|redPigment" : "C3RP",
                    "color|greenPigment" : "C3GP",
                    "width" : 12
                }
            ]
        }"""
    }
}
