// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.makers

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.KeyValueStore
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter
import spock.lang.Specification

public class FilteredAggregationMakerSpec extends Specification{

    private static final String DEPENDENT_METRIC_NAME = "totalPageViews"
    private static final String FILT_METRIC_NAME = "filteredPageViews"

    def "A filtered aggregation logical metric is made correctly"(){

        given: "The name of the metric the maker depends on, and the maker itself"

        Aggregation sumAggregation = new LongSumAggregation("tmp_metric", DEPENDENT_METRIC_NAME)
        Dimension dim = new KeyValueStoreDimension("d", "des", new LinkedHashSet<DimensionField>(), Mock(KeyValueStore), Mock(SearchProvider))
        Filter filter = new SelectorFilter(dim, "1")
        MetricMaker maker = new FilteredAggregationMaker(new MetricDictionary(), sumAggregation, filter)

        and: "The expected metric"

        Aggregation expectedAgg = new FilteredAggregation(FILT_METRIC_NAME, DEPENDENT_METRIC_NAME, sumAggregation, filter);
        LogicalMetric expectedMetric = new LogicalMetric(new TemplateDruidQuery([expectedAgg], [] as Set), new NoOpResultSetMapper(), FILT_METRIC_NAME)

        expect:
        maker.make(FILT_METRIC_NAME, []) == expectedMetric
    }
}
