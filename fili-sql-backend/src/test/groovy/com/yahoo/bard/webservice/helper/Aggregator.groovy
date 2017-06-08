// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.helper

import com.yahoo.bard.webservice.druid.model.aggregation.DoubleMaxAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleMinAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation

/**
 * Created by hinterlong on 6/7/17.
 */
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
}
