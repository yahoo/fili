// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.builders

import com.yahoo.bard.webservice.druid.model.MetricField
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation

class PostAggregator {
    public static ArithmeticPostAggregation ratioPostAggregator(
            String name,
            List<PostAggregation> fields
    ) {
        return new ArithmeticPostAggregation(
                name,
                ArithmeticPostAggregation.ArithmeticPostAggregationFunction.DIVIDE,
                fields
        )
    }

    public static FieldAccessorPostAggregation fieldAccessorPostAggregation (MetricField field) {
        return new FieldAccessorPostAggregation(field)
    }

    public static ArithmeticPostAggregation sumPostAggregator(
            String name,
            List<PostAggregation> fields
    ) {
        return new ArithmeticPostAggregation(
                name,
                ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS,
                fields
        )
    }
}
