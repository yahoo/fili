// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.makers

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.KeyValueStore
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolMetricImpl
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter

import spock.lang.Specification

public class FilteredAggregationMakerSpec extends Specification{

    private static final String DEPENDENT_METRIC_NAME = "totalPageViews"
    private static final String FILT_METRIC_NAME = "filteredPageViews"
    private static final LogicalMetricInfo FILTER_METRIC_INFO = new LogicalMetricInfo(FILT_METRIC_NAME)

    MetricDictionary metricDictionary = new MetricDictionary();
    LongSumMaker longSumMaker = new LongSumMaker(metricDictionary)

    def "A filtered aggregation logical metric is made correctly"(){
        given: "The name of the metric the maker depends on, and the maker itself"

        Dimension dim = new KeyValueStoreDimension("d", "des", [] as LinkedHashSet, Mock(KeyValueStore), Mock(SearchProvider))
        Filter filter = new SelectorFilter(dim, "1")

        MetricMaker maker = new FilteredAggregationMaker(metricDictionary, filter)
        LogicalMetric metric = longSumMaker.make("longSum", DEPENDENT_METRIC_NAME)
        metricDictionary.put("longSum", metric);

        and: "The expected metric"
        Aggregation expectedAgg = new FilteredAggregation(FILT_METRIC_NAME, new LongSumAggregation("longSum", DEPENDENT_METRIC_NAME), filter);
        LogicalMetric expectedMetric = new ProtocolMetricImpl(FILTER_METRIC_INFO, new TemplateDruidQuery([expectedAgg], [] as Set), new NoOpResultSetMapper())

        expect:
        maker.make(FILTER_METRIC_INFO, ["longSum"]) == expectedMetric
    }
}
