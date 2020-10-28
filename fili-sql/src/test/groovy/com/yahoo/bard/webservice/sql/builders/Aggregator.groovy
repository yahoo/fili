// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.builders

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.CountAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleMaxAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleMinAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongMaxAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongMinAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.sql.DruidQueryToSqlConverterSpec

class Aggregator {

    public static DoubleSumAggregation sum(String name) {
        return new DoubleSumAggregation(DruidQueryToSqlConverterSpec.API_PREPEND + name, name)
    }

    public static DoubleMaxAggregation max(String name) {
        return new DoubleMaxAggregation(DruidQueryToSqlConverterSpec.API_PREPEND + name, name)
    }

    public static DoubleMinAggregation min(String name) {
        return new DoubleMinAggregation(DruidQueryToSqlConverterSpec.API_PREPEND + name, name)
    }

    public static LongSumAggregation longSum(String name) {
        return new LongSumAggregation(DruidQueryToSqlConverterSpec.API_PREPEND + name, name)
    }

    public static LongMaxAggregation longMax(String name) {
        return new LongMaxAggregation(DruidQueryToSqlConverterSpec.API_PREPEND + name, name)
    }

    public static LongMinAggregation longMin(String name) {
        return new LongMinAggregation(DruidQueryToSqlConverterSpec.API_PREPEND + name, name)
    }

    public static CountAggregation count() {
        return new CountAggregation("count")
    }

    public static FilteredAggregation filteredLongSum(String name, Aggregation aggregation, Filter filter) {
        return new FilteredAggregation(DruidQueryToSqlConverterSpec.API_PREPEND + name, aggregation, filter);
    }
}
