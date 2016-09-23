// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.config.BardFeatureFlag.INTERSECTION_REPORTING
import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.THETA_SKETCH_ESTIMATE

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FuzzySetPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchSetOperationPostAggregation
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier

import spock.lang.Specification

class ThetaSketchNestedQuerySpec extends Specification {

    ThetaSketchIntersectionReportingResources resources
    boolean intersectionReportingState

    Map<String, Aggregation> aggNameToAggMap = [:]
    Map<String, String> nameMapper = [:]

    def setup() {
        intersectionReportingState = INTERSECTION_REPORTING.isOn()
        INTERSECTION_REPORTING.setOn(true)
        resources = new ThetaSketchIntersectionReportingResources().init()
    }

    def cleanup() {
        INTERSECTION_REPORTING.setOn(intersectionReportingState)
    }

    def "updateNestedQueryPostAggs appends the filter to the postAgg name if it is not of the type 'CONSTANT'"() {

        String filterSuffix = "-" + resources.filterObj.get("AND").toString().replaceAll("[^a-zA-Z0-9]", "");
        Set<PostAggregation> nestedQueryPostAggs = resources.dayAvgFoosTdq.nestedQuery.getPostAggregations()
        Set<PostAggregation> postAggregations = FieldConverterSupplier.metricsFilterSetBuilder.updateNestedQueryPostAggs(nestedQueryPostAggs, nameMapper, filterSuffix)

        expect:
        postAggregations.sort().first().getName() == "foos-countryidinUSINpropertyidin114125" || postAggregations.sort().first().getName() == "one"
        nameMapper.get("foos") == "foos-countryidinUSINpropertyidin114125"

    }

    def "updateQueryAggs updates the aggs from the outer query if the corresponding post aggs from inner query change"() {
        nameMapper.put("foos", "foos-propertyidin114125countryidinUSIN")
        Set<Aggregation> aggregationSet = FieldConverterSupplier.metricsFilterSetBuilder.updateQueryAggs(resources.dayAvgFoosTdq.getAggregations(), nameMapper, aggNameToAggMap)

        expect:
        aggregationSet.first().getName() == "foos_sum-propertyidin114125countryidinUSIN"
        aggregationSet.first().getFieldName() == "foos-propertyidin114125countryidinUSIN"
        aggNameToAggMap.get("foos_sum").getName() == "foos_sum-propertyidin114125countryidinUSIN"
        aggNameToAggMap.get("foos_sum").getFieldName() == "foos-propertyidin114125countryidinUSIN"
    }

    def "replaceOuterPostAggregation replaces the FieldAccessor in the outer query post agg if the corresponding outer agg changes"() {
        aggNameToAggMap.put("foos_sum", new DoubleSumAggregation("foos_sum-propertyidin114125countryidinUSIN", "foos-propertyidin114125countryidinUSIN"))
        ArithmeticPostAggregation pa = FieldConverterSupplier.metricsFilterSetBuilder.replacePostAggWithPostAggFromMap(resources.dayAvgFoosTdq.getPostAggregations().first(), aggNameToAggMap)

        expect:
        pa.getFields().first().equals(new FieldAccessorPostAggregation(aggNameToAggMap.get("foos_sum")))
    }

    def "Intersection reporting when Logical Metric has nested query"(){
        LinkedHashSet<LogicalMetric> logicalMetrics =  new DataApiRequest().generateLogicalMetrics("dayAvgFoos(AND(country|id-in[US,IN],property|id-in[114,125]))", resources.metricDict, resources.dimensionDict, resources.table)
        TemplateDruidQuery nestedQuery = logicalMetrics.first().templateDruidQuery.innerQuery

        Set<Aggregation> expectedNestedAggs = new HashSet<>()
        expectedNestedAggs.addAll(resources.fooNoBarFilteredAggregationSet)
        expectedNestedAggs.addAll(resources.fooRegFoosFilteredAggregationSet)
        nestedQuery.postAggregations.first().name == "foos-propertyidin114125countryidinUSIN"

        PostAggregation updatedNestedPostAgg
        for (PostAggregation pa : nestedQuery.postAggregations) {
            if(pa.type == THETA_SKETCH_ESTIMATE) {
                updatedNestedPostAgg = pa
                break
            }
        }

        expect:
        nestedQuery.getAggregations().sort().equals(expectedNestedAggs.sort())
        updatedNestedPostAgg.name == "foos-countryidinUSINpropertyidin114125"
        logicalMetrics.first().templateDruidQuery.aggregations.first().name == "foos_sum-countryidinUSINpropertyidin114125"
        logicalMetrics.first().templateDruidQuery.postAggregations.first().name == "dayAvgFoos"
    }

    def "metric filter on viz metric and expect children of unRegFoos have right sketch operation function"() {
        LinkedHashSet<LogicalMetric> logicalMetrics = new DataApiRequest().generateLogicalMetrics("viz(AND(country|id-in[US,IN],property|id-in[14,125]))", resources.metricDict, resources.dimensionDict, resources.table)
        ArithmeticPostAggregation postAggregation = logicalMetrics.first().templateDruidQuery.postAggregations.first()

        FuzzySetPostAggregation unRegFoo1;
        for (PostAggregation pa : postAggregation.fields) {
            if (pa instanceof FuzzySetPostAggregation) {
                unRegFoo1 = pa
                break
            }
        }
        List<ThetaSketchSetOperationPostAggregation> unRegFooChildren =
                ((ThetaSketchSetOperationPostAggregation) unRegFoo1.field).fields

        expect:
        unRegFooChildren[0].func.equals(SketchSetOperationPostAggFunction.INTERSECT)
        unRegFooChildren[1].func.equals(SketchSetOperationPostAggFunction.UNION)
    }

    def "When metrics of Ratio category are filtered, BadApiException is thrown"() {
        when:
        new DataApiRequest().generateLogicalMetrics("ratioMetric(AND(country|id-in[US,IN],property|id-in[14,125]))", resources.metricDict, resources.dimensionDict, resources.table)

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == "Metric filtering is not supported for metric 'ratioMetric' as it belongs to Ratios' category"
    }

    def "The dimensions returned from the filtered nested logical metric are correct"() {
        LinkedHashSet<LogicalMetric> logicalMetrics = new DataApiRequest().generateLogicalMetrics("dayAvgFoos(AND(country|id-in[US,IN],property|id-in[114,125]))", resources.metricDict, resources.dimensionDict, resources.table)

        expect:
        logicalMetrics.first().templateDruidQuery.metricDimensions.sort() == [resources.propertyDim, resources.countryDim].sort()
    }
}
