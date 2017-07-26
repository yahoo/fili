// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.builders

import com.yahoo.bard.webservice.druid.model.aggregation.DoubleMaxAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleMinAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongMaxAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongMinAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation

class Aggregator {

    public static DoubleSumAggregation sum(String name) {
        return new DoubleSumAggregation(name, name)
    }

    public static DoubleMaxAggregation max(String name) {
        return new DoubleMaxAggregation(name, name)
    }

    public static DoubleMinAggregation min(String name) {
        return new DoubleMinAggregation(name, name)
    }

    public static LongSumAggregation longSum(String name) {
        return new LongSumAggregation(name, name)
    }

    public static LongMaxAggregation longMax(String name) {
        return new LongMaxAggregation(name, name)
    }

    public static LongMinAggregation longMin(String name) {
        return new LongMinAggregation(name, name)
    }
}
