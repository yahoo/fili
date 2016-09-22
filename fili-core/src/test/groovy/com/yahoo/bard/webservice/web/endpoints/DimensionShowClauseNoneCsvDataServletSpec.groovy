// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

class DimensionShowClauseNoneCsvDataServletSpec extends BaseDimensionShowClauseCsvDataServletSpec {

    @Override
    String getTarget() {
        return "data/shapes/week/color;show=none"
    }

    @Override
    String getExpectedApiResponse() {
        """dateTime,color,width
          |\"2014-06-02 00:00:00.000\",Color1,10
          |\"2014-06-02 00:00:00.000\",Color2,11
          |\"2014-06-02 00:00:00.000\",Color3,12
          |""".stripMargin()
    }
}
