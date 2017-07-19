// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionRowNotFoundException;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction;
import com.yahoo.bard.webservice.table.LogicalTable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An interface containing helper methods to build filtered metrics.
 */
public interface MetricsFilterSetBuilder {

    /**
     * This method checks if the metric query contains duplicate metrics and throws a BadApiRequestException in case
     * it does find duplicate metrics.
     *
     * @param  metricsJsonArray  A JSONArray containing JSONObjects with name and filter. For Example :
     * <pre>{@code [{"filter":{"AND":{"dim2|id-in":["abc","xyz"],"dim3|id-in":["mobile","tablet"]}},"name":"metric1"},
     * {"filter":{},"name":"metric2"}] }</pre>
     *
     * @throws  BadApiRequestException Invalid metric query if the metric query has
     * duplicate metrics
     */
    void validateDuplicateMetrics(ArrayNode metricsJsonArray) throws BadApiRequestException;

    /**
     * Provides filter wrapped logical metric for the given logical metric.
     *
     * @param logicalMetric  The LogicalMetric that needs to be transformed into a filtered Logical Metric
     * @param metricFilterObject  A JSON object containing the filter for a given metric. For example:
     * <pre>{@code {"AND":{"dim2|id-in":["abc","xyz"],"dim3|id-in":["mobile","tablet"]}},"name":"metric"} </pre>
     * @param dimensionDictionary  Dimension dictionary to look the dimension up in
     * @param table  The logical table for the data request
     * @param apiRequest  The data api request that will be used to generate the filters
     *
     * @return A Logical Metric that is filtered
     * @throws DimensionRowNotFoundException if the dimension mentioned in the
     * metric filter is not found
     */
    LogicalMetric getFilteredLogicalMetric(
            LogicalMetric logicalMetric,
            JsonNode metricFilterObject,
            DimensionDictionary dimensionDictionary,
            LogicalTable table,
            DataApiRequest apiRequest
    ) throws DimensionRowNotFoundException;

    /**
     * A method to update outer query aggregations and postAggregations.
     *
     * @param outerQuery  Outer query of a Logical Metric
     * @param oldFieldNameToNewFieldNameMap  Map which contains mapping old name --&gt; new name of the post aggs from
     * the nested query. Eg: foo --&gt; foo-dim1334dim2445
     *
     * @return TemplateDruidQuery which is generated from updated aggs, post aggs and the given nested query
     */
    TemplateDruidQuery updateOuterQuery(
            TemplateDruidQuery outerQuery,
            Map<String, String> oldFieldNameToNewFieldNameMap
    );

    /**
     * Update the outer query aggs if their respective inner post agg names are updated.
     *
     * @param outerAggregations  Outer query aggregations
     * @param oldFieldNameToNewFieldNameMap  Map which contains mapping old name --&gt; new name of the post aggs from
     * the nested query. Eg: foo --&gt; foo-dim1334dim2445
     * @param oldNameToNewAggregationMapping  Empty Map to keep old name as key and new aggregation as value
     *
     * @return Updated outer query aggregations
     */
    Set<Aggregation> updateQueryAggs(
            Set<Aggregation> outerAggregations,
            Map<String, String> oldFieldNameToNewFieldNameMap,
            Map<String, Aggregation> oldNameToNewAggregationMapping
    );

    /**
     * Update the nested query post agg names if they are not of the type CONSTANT.
     *
     * @param nestedQueryPostAggs  A set of all the nested query post aggregations
     * @param oldFieldNameToNewFieldNameMap  Empty Map to store the mapping old name --&gt; new name of the post aggs.
     * Eg: foo --&gt; foo-dim1334dim2445
     * @param filterSuffix  Suffix which is appended to the postAgg name to generate new postAgg name
     *
     * @return updated inner query post aggregations
     */
    Collection<PostAggregation> updateNestedQueryPostAggs(
            Collection<PostAggregation> nestedQueryPostAggs,
            Map<String, String> oldFieldNameToNewFieldNameMap,
            String filterSuffix
    );

    /**
     * Method to update a given query by changing aggregators to filteredAggregators and updating postAggs to reference
     * the filteredAggregators.
     *
     * @param query  Template druid query of a given Logical Metric. Can also be the nested query but can only contain
     * Sketch aggregations.
     * @param metricFilterObject  Metric filter associated with the metric
     * @param dimensionDictionary  Dimension dictionary to look the dimension up in
     * @param table  The logical table for the data request
     * @param apiRequest  The data api request that will be used to generate the filters
     *
     * @return updated query which contains filtered aggregations
     * @throws DimensionRowNotFoundException if the dimension row in the metric
     * filter is not found.
     */
    TemplateDruidQuery updateTemplateDruidQuery(
            TemplateDruidQuery query,
            JsonNode metricFilterObject,
            DimensionDictionary dimensionDictionary,
            LogicalTable table,
            DataApiRequest apiRequest
    ) throws DimensionRowNotFoundException;

    /**
     * A method to replace postAggs with new postAggs that access the intersection or union of filteredAggregators.
     *
     * @param func  Sketch operation function Ex: INTERSECT, UNION
     * @param postAggregation  Post aggregation which needs to be replaced
     * @param filteredAggDictionary  Dictionary which holds aggregator name as key and list of FilteredAggregation as
     * value
     *
     * @return A new PostAggregation formed by replacing its FieldAccessor with intersection of its
     * filtered aggregations and replacing each of the intermediate post aggregations with new post aggregations formed
     * from its new children
     */
    PostAggregation replacePostAggregation(
            SketchSetOperationPostAggFunction func,
            PostAggregation postAggregation,
            Map<String, List<FilteredAggregation>> filteredAggDictionary
    );

    /**
     * Takes a post aggregation and updates its FieldAccessor if its fieldName is present in the map changed.
     *
     * @param postAggregation  Outer query post aggregation that needs to be updated
     * @param oldNameToNewAggregationMapping  Map with old aggname as key and new aggregation as value
     *
     * @return updated post aggregation if the referencing aggregation name has changed; Otherwise, as it is
     */
    PostAggregation replacePostAggWithPostAggFromMap(
            PostAggregation postAggregation,
            Map<String, Aggregation> oldNameToNewAggregationMapping
    );

    /**
     * For a given aggregator, this method applies the filter and returns a set of filtered aggregations.
     *
     * @param filter  Filter to be used in generating FilteredAggregation
     * @param aggregation  Aggregation that needs to be wrapped with Filter in order to generate the
     * FilteredAggregation
     * @param dimensionDictionary  Dimension dictionary to look the dimension up in
     * @param table  The logical table for the data request
     * @param apiRequest  The data api request that will be used to generate the filters
     *
     * @return A set of FilteredAggregators for the given aggregator and Filter
     * @throws DimensionRowNotFoundException if the dimension row in the metric
     * filter is not found.
     */
    Set<FilteredAggregation> getFilteredAggregation(
            JsonNode filter,
            Aggregation aggregation,
            DimensionDictionary dimensionDictionary,
            LogicalTable table,
            DataApiRequest apiRequest
    ) throws DimensionRowNotFoundException;

    /**
     * Method to prepare filter string.
     *
     * @param filterString  Single dimension filter string
     *
     * @return Metric name extension created for the filter
     */
    String generateMetricName(String filterString);

    /**
     * Helper method that calls {@link #gatherFilterDimensions(Filter, Set)} by passing in filter and an empty HashSet.
     *
     * @param filter  Filter object whose dimensions are to be collected
     *
     * @return Set of dimensions belonging to a Filter
     */
    Set<Dimension> gatherFilterDimensions(Filter filter);

    /**
     * Method to get dimensions from a Filter object.
     *
     * @param filter  filter whose dimensions need to be collected
     * @param dimensions  Set of dimensions belonging to a filter. Empty when the method is first called.
     *
     * @return Set of dimensions belonging to a Filter
     */
    Set<Dimension> gatherFilterDimensions(Filter filter, Set<Dimension> dimensions);
}
