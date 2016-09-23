// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

class DimensionShowClauseCsvDataServletSpec extends BaseDimensionShowClauseCsvDataServletSpec {

    @Override
    String getTarget() {
        return "data/shapes/week/color;show=id,bluePigment"
    }

    @Override
    String getExpectedApiResponse() {
        """dateTime,color|id,color|bluePigment,width
          |\"2014-06-02 00:00:00.000\",Color1,C1BP,10
          |\"2014-06-02 00:00:00.000\",Color2,C2BP,11
          |\"2014-06-02 00:00:00.000\",Color3,C3BP,12
          |""".stripMargin()
    }
}
