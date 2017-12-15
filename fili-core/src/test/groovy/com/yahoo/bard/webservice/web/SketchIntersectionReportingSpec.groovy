// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.config.BardFeatureFlag.INTERSECTION_REPORTING

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.filterbuilders.DefaultDruidFilterBuilder
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchEstimatePostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FuzzySetPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggregation
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier
import com.yahoo.bard.webservice.web.apirequest.utils.TestingDataApiRequestImpl

import spock.lang.Specification
/**
 * @deprecated  To consider the latest version of sketch Library.
 * This class is replaced by ThetaSketchIntersectionReportingSpec class
 */
@Deprecated
class SketchIntersectionReportingSpec extends Specification {

    SketchIntersectionReportingResources resources
    boolean intersectionReportingState

    def setup() {
        intersectionReportingState = INTERSECTION_REPORTING.isOn()
        INTERSECTION_REPORTING.setOn(true)
        resources = new SketchIntersectionReportingResources().init()
    }

    def cleanup() {
        INTERSECTION_REPORTING.setOn(intersectionReportingState)
    }

    def "When the format of the metric filter is invalid, BadApiRequestException is thrown"(){
        when:
        new TestingDataApiRequestImpl().generateLogicalMetrics(
                "foos(AND(country|id-in[US,IN]property|id-in[14,125]))",
                resources.metricDict,
                resources.dimensionDict,
                resources.table
        )

        then:
        Exception e = thrown(BadApiRequestException)
        e.message == "Filter expression 'country|id-in[US,IN]property|id-in[14,125]' is invalid."

    }

    def "When the API query contains duplicate metrics, BadApiRequestException is thrown"(){
        when:
        new TestingDataApiRequestImpl().generateLogicalMetrics(
                "foos,foos(AND(country|id-in[US,IN],property|id-in[14,125]))",
                resources.metricDict,
                resources.dimensionDict,
                resources.table
        )

        then:
        Exception e = thrown(BadApiRequestException)
        e.message == ErrorMessageFormat.DUPLICATE_METRICS_IN_API_REQUEST.format("[foos]")
    }

    def "When the queried metric is not present in Metric Dictionary, BadApiRequestException is thrown"() {
        when:
        new TestingDataApiRequestImpl().generateLogicalMetrics(
                "dinga",
                resources.metricDict,
                resources.dimensionDict,
                resources.table
        )

        then:
        Exception e = thrown(BadApiRequestException)
        e.message == ErrorMessageFormat.METRICS_UNDEFINED.format("[dinga]")
    }

    def "When metric filter contains 'OR' condition, BadApiRequestException is thrown"(){
        when:
        new TestingDataApiRequestImpl().generateLogicalMetrics(
                "foos(OR(country|id-in[US,IN],property|id-in[14,125]))",
                resources.metricDict,
                resources.dimensionDict,
                resources.table
        )

        then:
        Exception e = thrown(BadApiRequestException)
        e.message == "Filter dimension 'ORcountry' does not exist."
    }

    def "When the INTERSECTION_REPORTING flag is enabled and the query contains unfiltered metrics, the Logical Metrics returned are equal to the Logical Metrics from the Metric Dictionary"() {
        Set<LogicalMetric> logicalMetrics = new TestingDataApiRequestImpl().generateLogicalMetrics(
                "pageViews,foos",
                resources.metricDict,
                resources.dimensionDict,
                resources.table
        )
        HashSet<Dimension> expected =
                ["pageViews", "foos"].collect { String name ->
                    LogicalMetric metric = resources.metricDict.get(name)
                    assert metric?.name == name
                    metric
                }

        expect:
        logicalMetrics == expected
    }

    def "When metric filter contains invalid dimension, BadApiRequestException is thrown"(){
        when:
        new TestingDataApiRequestImpl().generateLogicalMetrics(
                "foos(AND(country1|id-in[US,IN],property|id-in[14,125]))",
                resources.metricDict,
                resources.dimensionDict,
                resources.table
        )

        then:
        Exception e = thrown(BadApiRequestException)
        e.message == "Filter dimension 'country1' does not exist."
    }

    def "When filtered metric has no post aggs, the estimate of intersection of its Filtered Aggregators is used as its post agg"(){
        TemplateDruidQuery templateDruidQuery = FieldConverterSupplier.metricsFilterSetBuilder.updateTemplateDruidQuery(
                resources.metricDict.get("regFoos").templateDruidQuery,
                resources.filterObj,
                resources.dimensionDict,
                resources.table,
                new DefaultDruidFilterBuilder()
        )

        expect:
        ((SketchEstimatePostAggregation) templateDruidQuery.postAggregations.first()).field == resources.fooRegFoosPostAggregationInterim
    }

    def "updateTemplateDruidQuery replaces the aggs with filteredAggs and postAggs with intersection or union of its filteredAggs"(){

        TemplateDruidQuery templateDruidQuery = FieldConverterSupplier.metricsFilterSetBuilder.updateTemplateDruidQuery(
                resources.metricDict.get("foos").templateDruidQuery,
                resources.filterObj,
                resources.dimensionDict,
                resources.table,
                new DefaultDruidFilterBuilder()
        )

        Set<Aggregation> aggregations = templateDruidQuery.aggregations;
        Aggregation aggregation = aggregations.first()

        FuzzySetPostAggregation postAggregation = templateDruidQuery.postAggregations.first()
        SketchSetOperationPostAggregation setOperationPostAggregation = postAggregation.field

        expect:
        //The number of Filtered Aggregations should be  number of aggs * number of filters
        aggregations.size() == 4
        aggregation instanceof FilteredAggregation
        setOperationPostAggregation.fields == [resources.fooNoBarPostAggregationInterim, resources.fooRegFoosPostAggregationInterim]
    }

    def "replacePostAggregation replaces the filedAccesors of foo postAgg with intersection of its Filtered Aggregators"(){
        FuzzySetPostAggregation replacedPostAgg =
                (FuzzySetPostAggregation) FieldConverterSupplier.metricsFilterSetBuilder.replacePostAggregation(
                        SketchSetOperationPostAggFunction.INTERSECT,
                        resources.fooPostAggregation,
                        resources.interimPostAggDictionary
                )
        SketchSetOperationPostAggregation setOperationPostAggregation = replacedPostAgg.field

        expect:
        setOperationPostAggregation.fields == [resources.fooNoBarPostAggregationInterim, resources.fooRegFoosPostAggregationInterim]
    }

    def "getFilteredAggregation returns a set of filteredAggregations for a given aggregation and Filter object"(){
        expect:
        resources.fooNoBarFilteredAggregationSet.first() == new FilteredAggregation(
                "fooNoBar-country_id_in_US_IN",
                resources.fooNoBarAggregation,
                resources.filter
        )
    }

    def "When invalid(non-sketch) metric is used for filtering, IllegalArgumentException is thrown "(){
        when:
        new TestingDataApiRequestImpl().generateLogicalMetrics(
                "pageViews(AND(country|id-in[US,IN],property|id-in[news,sports]))",
                resources.metricDict,
                resources.dimensionDict,
                resources.table
        )

        then:
        Exception e = thrown(IllegalArgumentException)
        e.message == String.format("Aggregation type should be sketchCount: but Aggregation type for pageViews is: longSum")
    }

    def "When API request contains filtered metrics, the Logical Metric returned by generateLogicalMetrics is filtered and therefore not equal to the Logical Metric from the Metric dictionary "(){
        LinkedHashSet<LogicalMetric> logicalMetrics =  new TestingDataApiRequestImpl().generateLogicalMetrics(
                "foos(AND(country|id-in[US,IN],property|id-in[14,125]))",
                resources.metricDict,
                resources.dimensionDict,
                resources.table
        )

        HashSet<Dimension> expected =
                ["foos"].collect { String name ->
                    LogicalMetric metric = resources.metricDict.get(name)
                    assert metric?.name == name
                    metric
                }

        expect:
        logicalMetrics != expected
    }

    def "An exception is thrown when validateMetrics is passed an intersection expression using invalid metrics"(){
        LinkedHashSet<LogicalMetric> logicalMetrics =  new TestingDataApiRequestImpl().generateLogicalMetrics(
                "regFoos(AND(country|id-in[US,IN],property|id-in[14,125]))",
                resources.metricDict,
                resources.dimensionDict,
                resources.table
        )

        when:
        new TestingDataApiRequestImpl().validateMetrics(logicalMetrics,resources.table)

        then:
        String expectedMessage = "Requested metric(s) '[regFoos]' are not supported by the table 'NETWORK'."
        Exception e = thrown(BadApiRequestException)
        e.message == expectedMessage

    }

    def "No exception is thrown when validateMetrics is passed an intersection expression using valid metrics"(){
        LinkedHashSet<LogicalMetric> logicalMetrics =  new TestingDataApiRequestImpl().generateLogicalMetrics(
                "foos(AND(country|id-in[US,IN],property|id-in[14,125]))",
                resources.metricDict,
                resources.dimensionDict,
                resources.table
        )

        when:
        new TestingDataApiRequestImpl().validateMetrics(logicalMetrics, resources.table)

        then:
        noExceptionThrown()
    }

    def "The dimensions returned from the filtered logical metric are correct"() {
        LinkedHashSet<LogicalMetric> logicalMetrics =  new TestingDataApiRequestImpl().generateLogicalMetrics(
                "foos(AND(country|id-in[US,IN],property|id-in[14,125]))",
                resources.metricDict,
                resources.dimensionDict,
                resources.table
        )

        expect:
        logicalMetrics.first().templateDruidQuery.metricDimensions.sort() == [resources.propertyDim, resources.countryDim].sort()
    }
}
