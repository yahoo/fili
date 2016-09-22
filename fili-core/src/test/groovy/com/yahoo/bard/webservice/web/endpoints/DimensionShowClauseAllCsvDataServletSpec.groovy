// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

class DimensionShowClauseAllCsvDataServletSpec extends BaseDimensionShowClauseCsvDataServletSpec {

    @Override
    String getTarget() {
        return "data/shapes/week/color;show=all"
    }

    @Override
    String getExpectedApiResponse() {
        """dateTime,color|id,color|desc,color|bluePigment,color|redPigment,color|greenPigment,width
          |\"2014-06-02 00:00:00.000\",Color1,Color1Desc,C1BP,C1RP,C1GP,10
          |\"2014-06-02 00:00:00.000\",Color2,Color2Desc,C2BP,C2RP,C2GP,11
          |\"2014-06-02 00:00:00.000\",Color3,Color3Desc,C3BP,C3RP,C3GP,12
          |""".stripMargin()
    }
}
