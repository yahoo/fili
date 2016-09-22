// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

class DimensionShowClauseDataServletSpec extends BaseDimensionShowClauseDataServletSpec {

    @Override
    String getTarget() {
        return "data/shapes/week/color;show=id,bluePigment"
    }

    @Override
    String getExpectedApiResponse() {
        """{
            "rows" : [
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "color|id" : "Color1",
                    "color|bluePigment" : "C1BP",
                    "width" : 10
                },
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "color|id" : "Color2",
                    "color|bluePigment" : "C2BP",
                    "width" : 11
                },
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "color|id" : "Color3",
                    "color|bluePigment" : "C3BP",
                    "width" : 12
                }
            ]
        }"""
    }
}
