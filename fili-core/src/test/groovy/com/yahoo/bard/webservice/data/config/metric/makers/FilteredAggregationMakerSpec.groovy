// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.makers

import com.yahoo.bard.webservice.data.config.names.TestApiMetricName
import com.yahoo.bard.webservice.data.config.names.TestDruidMetricName
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
import com.yahoo.bard.webservice.data.time.AllGranularity
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.druid.model.MetricField
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter

import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.Predicate

public class FilteredAggregationMakerSpec extends Specification{

    private static final String LONGSUM_METRIC_NAME = "longsum"
    private static final String DRUID_COLUMN_NAME = "test"
    private static final String FILT_METRIC_NAME = "filteredPageViews"
    private static final LogicalMetricInfo FILTER_METRIC_INFO = new LogicalMetricInfo(FILT_METRIC_NAME)

    MetricDictionary metricDictionary = new MetricDictionary();
    LongSumMaker longSumMaker = new LongSumMaker(metricDictionary)

    List<Granularity> granularities;

    MetricMaker maker
    Dimension dim
    Filter filter

    def setup() {
        granularities = new ArrayList<>(Arrays.asList(DefaultTimeGrain.values()))
        granularities.add(AllGranularity.INSTANCE)

        LogicalMetricImpl baseMetric = longSumMaker.make(LONGSUM_METRIC_NAME, DRUID_COLUMN_NAME)
        metricDictionary.put(LONGSUM_METRIC_NAME, baseMetric);

        metricDictionary.put("weekOnly", baseMetric.withValidFor({it == DefaultTimeGrain.WEEK}))
        metricDictionary.put("notAll", baseMetric.withValidFor({it != AllGranularity.INSTANCE}))


        ConstantMaker constantMaker = new ConstantMaker(metricDictionary)
        metricDictionary.put("one", constantMaker.make(new LogicalMetricInfo("one"), ["1"]))

        dim = new KeyValueStoreDimension("d", "des", [] as LinkedHashSet, Mock(KeyValueStore), Mock(SearchProvider))
        filter = new SelectorFilter(dim, "1")
        maker = new FilteredAggregationMaker(metricDictionary, filter)
    }

    @Unroll
    def "Filtered maker produces a nested aggregation and wraps the granularity predicate"(){
        given: "A filtered aggregation maker"
        LogicalMetric baseMetric = metricDictionary.get(baseMetricName)
        Aggregation aggregation = (Aggregation) baseMetric.getTemplateDruidQuery().getMetricField(metricField)
        Aggregation expectedAgg = new FilteredAggregation(FILTER_METRIC_INFO.name, aggregation, filter);
        TemplateDruidQuery templateDruidQuery = new TemplateDruidQuery([expectedAgg], [] as Set)

        expect:
        LogicalMetric result = maker.make(FILTER_METRIC_INFO, [baseMetricName])
        result.templateDruidQuery == templateDruidQuery
        result.calculation == baseMetric.calculation
        result.logicalMetricInfo == FILTER_METRIC_INFO
        granularities.stream().allMatch() {result.isValidFor(it) == baseMetric.isValidFor(it)}

        where:
        baseMetricName      | metricField
        LONGSUM_METRIC_NAME | LONGSUM_METRIC_NAME
        "weekOnly"          | LONGSUM_METRIC_NAME
        "notAll"            | LONGSUM_METRIC_NAME
    }

    def "Non agg dependant metric blows up"(){
        when:
        LogicalMetric result = maker.make(FILTER_METRIC_INFO, ["one"])

        then:
        Exception e = thrown(IllegalArgumentException)
        e.getMessage().startsWith( "FilteredAggregationMaker")
    }
}
