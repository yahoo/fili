// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DUPLICATE_METRICS_IN_API_REQUEST;

import com.yahoo.bard.webservice.data.config.metric.makers.SketchSetOperationHelper;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionRowNotFoundException;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.filter.MultiClauseFilter;
import com.yahoo.bard.webservice.druid.model.filter.NotFilter;
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter;
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchEstimatePostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.WithFields;
import com.yahoo.bard.webservice.table.LogicalTable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class containing helper methods to build filtered metric.
 *
 * @deprecated  To consider the latest version of sketch Library.
 * This class is replaced by FilteredThetaSketchMetricsHelper class
 */
@Deprecated
public class FilteredSketchMetricsHelper implements MetricsFilterSetBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(FilteredSketchMetricsHelper.class);
    private static final String ALPHANUMERIC_REGEX = "[^a-zA-Z0-9]";

    @Override
    public void validateDuplicateMetrics(ArrayNode metricsJsonArray) {
        Set<String> metricsList = new HashSet<>();
        List<String> duplicateMetrics = new ArrayList<>();

        for (int i = 0; i < metricsJsonArray.size(); i++) {
            String metricName = metricsJsonArray.get(i).get("name").asText();
            boolean status = metricsList.add(metricName);
            if (!status) {
                duplicateMetrics.add(metricName);
            }
        }
        if (!duplicateMetrics.isEmpty()) {
            LOG.debug(DUPLICATE_METRICS_IN_API_REQUEST.logFormat(duplicateMetrics.toString()));
            throw new BadApiRequestException(DUPLICATE_METRICS_IN_API_REQUEST.format(duplicateMetrics.toString()));
        }
    }

    @Override
    public LogicalMetric getFilteredLogicalMetric(
            LogicalMetric logicalMetric,
            JsonNode metricFilterObject,
            DimensionDictionary dimensionDictionary,
            LogicalTable table,
            DataApiRequest apiRequest
    ) throws DimensionRowNotFoundException {

        TemplateDruidQuery templateDruidQuery = logicalMetric.getTemplateDruidQuery();

        if (templateDruidQuery.isNested()) {
            //If the Logical Metric is nested, update the aggs and post aggs of the nested query.
            TemplateDruidQuery newInnerQuery = updateTemplateDruidQuery(
                    templateDruidQuery.getInnerQuery(),
                    metricFilterObject,
                    dimensionDictionary,
                    table,
                    apiRequest
            );

            String filterSuffix = "-" + metricFilterObject.get("AND").asText().replaceAll(ALPHANUMERIC_REGEX, "");

            //innerPostAggToOuterAggMap stores the mapping of old postAgg to new postAgg name. In filtering of metrics,
            //the name of postAggs(all postAggs other than CONSTANT type) in the nested query is appended with a filter
            //string in order to avoid name conflicts in the outer query.
            //Example entry-set: foo --> foo-ANDdim122dim244
            Map<String, String> innerPostAggToOuterAggMap = new HashMap<>();

            newInnerQuery = newInnerQuery.withPostAggregations(
                    updateNestedQueryPostAggs(
                            newInnerQuery.getPostAggregations(),
                            innerPostAggToOuterAggMap,
                            filterSuffix
                    )
            );

            //Update the outer query to access the correct postAggs from the nested query
            templateDruidQuery = updateOuterQuery(
                    templateDruidQuery.withInnerQuery(newInnerQuery),
                    innerPostAggToOuterAggMap
            );

        } else {
            //non-nested queries
            templateDruidQuery = updateTemplateDruidQuery(
                    templateDruidQuery,
                    metricFilterObject,
                    dimensionDictionary,
                    table,
                    apiRequest
            );
        }
        //build new LogicalMetric and return
        return new LogicalMetric(templateDruidQuery, logicalMetric.getCalculation(), logicalMetric.getName());
    }

    /**
     * A method to update outer query aggregations &amp; postAggregations.
     *
     * @param outerQuery  Outer query of a Logical Metric
     * @param oldFieldNameToNewFieldNameMap  Map which contains mapping old name {@literal -->} new name of the post
     * aggs from the nested query. Eg: foo {@literal -->} foo-dim1334dim2445
     *
     * @return TemplateDruidQuery which is generated from updated aggs, post aggs and the given nested query
     */
    @Override
    public TemplateDruidQuery updateOuterQuery(
            TemplateDruidQuery outerQuery,
            Map<String, String> oldFieldNameToNewFieldNameMap
    ) {
        Map<String, Aggregation> oldNameToNewAggregationMapping = new HashMap<>();
        Set<Aggregation> updatedOuterAggs = updateQueryAggs(
                outerQuery.getAggregations(),
                oldFieldNameToNewFieldNameMap,
                oldNameToNewAggregationMapping
        );

        //Update the FieldAccessors from the outer query post aggs to access the correct aggs.
        Set<PostAggregation> updateOuterPostAggs = new HashSet<>();
        for (PostAggregation postAggregation: outerQuery.getPostAggregations()) {
            updateOuterPostAggs.add(replacePostAggWithPostAggFromMap(postAggregation, oldNameToNewAggregationMapping));
        }

        //create new TDQ using updated aggs, updatedPostAggs, updatedInnerQuery and timegrain of outerQuery
        return new TemplateDruidQuery(
                updatedOuterAggs,
                updateOuterPostAggs,
                outerQuery.getInnerQuery(),
                outerQuery.getTimeGrain()
        );
    }

    /**
     * Update the outer query aggs if their respective inner post agg names are updated.
     *
     * @param outerAggregations  Outer query aggregations
     * @param oldFieldNameToNewFieldNameMap  Map which contains mapping old name {@literal -->} new name of the
     * post aggs from the nested query. Eg: foo {@literal -->} foo-dim1334dim2445
     * @param oldNameToNewAggregationMapping  Empty Map to keep old name as key and new aggregation as value
     *
     * @return Updated outer query aggregations
     */
    @Override
    public Set<Aggregation> updateQueryAggs(
            Set<Aggregation> outerAggregations,
            Map<String, String> oldFieldNameToNewFieldNameMap,
            Map<String, Aggregation> oldNameToNewAggregationMapping
    ) {
        Set<Aggregation> aggregationSet = new HashSet<>();
        for (Aggregation agg: outerAggregations) {
            //If the agg fieldName exists in the map, then its name has changed in the innerQuery post agg
            if (oldFieldNameToNewFieldNameMap.containsKey(agg.getFieldName())) {

                //fieldName = foo-dim1114dim2345
                String newFieldName = oldFieldNameToNewFieldNameMap.get(agg.getFieldName());

                // ex: name foo_sum --> 'foo_sum-dim1114dim2345'
                String newName = agg.getName().concat("-").concat(newFieldName.split("-")[1]);

                //Generate new aggreation with new name and new field name
                Aggregation aggregation = agg.withName(newName).withFieldName(newFieldName);

                //Add updated aggreation with new name and field name
                aggregationSet.add(aggregation);
                //oldNameToNewAggregationMapping contains old agg name as key and new agg as value
                oldNameToNewAggregationMapping.put(agg.getName(), aggregation);
            } else {
                aggregationSet.add(agg);
            }
        }
        return aggregationSet;
    }

    /**
     * Update the nested query post agg names if they are not of the type CONSTANT.
     *
     * @param nestedQueryPostAggs  A set of all the nested query post aggregations
     * @param oldFieldNameToNewFieldNameMap  Empty Map to store the mapping old name {@literal -->} new name
     * of the post aggs. Eg: foo {@literal -->} foo-dim1334dim2445
     * @param filterSuffix  Suffix which is appended to the postAgg name to generate new postAgg name
     *
     * @return updated inner query post aggregations
     */
    @Override
    public Collection<PostAggregation> updateNestedQueryPostAggs(
            Collection<PostAggregation> nestedQueryPostAggs,
            Map<String, String> oldFieldNameToNewFieldNameMap,
            String filterSuffix
    ) {
        Set<PostAggregation> postAggregationSet = new HashSet<>();

        for (PostAggregation postAggregation: nestedQueryPostAggs) {
            if (postAggregation instanceof ConstantPostAggregation) {
                postAggregationSet.add(postAggregation);
            } else {
                //We want to change the names of all postAggs other than CONSTANT type
                oldFieldNameToNewFieldNameMap.put(
                        postAggregation.getName(),
                        postAggregation.getName().concat(filterSuffix)
                );
                postAggregationSet.add(
                        postAggregation.withName(oldFieldNameToNewFieldNameMap.get(postAggregation.getName()))
                );
            }
        }
        return postAggregationSet;
    }

    @Override
    public TemplateDruidQuery updateTemplateDruidQuery(
            TemplateDruidQuery query,
            JsonNode metricFilterObject,
            DimensionDictionary dimensionDictionary,
            LogicalTable table,
            DataApiRequest apiRequest
    ) throws DimensionRowNotFoundException {

        Set<PostAggregation> postAggregations = query.getPostAggregations();
        Set<PostAggregation> updatedPostAggs = new HashSet<>();
        Set<Aggregation> updatedAggs = new HashSet<>();
        //filteredAgg dictionary contains a mapping of the old agg name to the new FilteredAggregation
        Map<String, List<FilteredAggregation>> filteredAggDictionary = new HashMap<>();

        //Retrieving all aggregations for the given logical metric; So we can wrap it up with respective filters
        for (Aggregation aggregation : query.getAggregations()) {
            //Takes an Aggregation and checks if is a sketch or not. It not, throw an error
            if (!aggregation.isSketch()) {
                String message = String.format(
                        "Aggregation type should be sketchCount: but Aggregation type for %s is: %s",
                        aggregation.getFieldName(),
                        aggregation.getType()
                );
                LOG.error(message);
                throw new IllegalArgumentException(message);
            }

            //For each aggregation, apply the respective filters
            Set<FilteredAggregation> filteredAggregatorSet = getFilteredAggregation(
                    metricFilterObject,
                    aggregation,
                    dimensionDictionary,
                    table,
                    apiRequest
            );

            updatedAggs.addAll(filteredAggregatorSet);
            String aggregationName = aggregation.getName();
            List<FilteredAggregation> filteredAggregatorList = new ArrayList<>(filteredAggregatorSet);
            filteredAggDictionary.put(aggregationName, filteredAggregatorList);

            if (postAggregations.isEmpty()) {
                //For non derived metric, there would be single aggregator and no post aggregator.
                //In this case the post aggregator for the new logical metric will be the sketch estimate of the
                //post agg derived by applying a set operation(For eg: AND, OR, NOT) on all its filtered aggregators
                updatedPostAggs.add(
                        new SketchEstimatePostAggregation(
                                aggregationName,
                                SketchSetOperationHelper.makePostAggFromAgg(
                                        SketchSetOperationPostAggFunction.INTERSECT,
                                        aggregationName,
                                        filteredAggregatorList
                                )
                        )
                );
            }
        }

        //If the Logical metric has post aggregations, we need to replace them with new postAggs containing intersection
        //or union of FilteredAggregators

        for (PostAggregation postAggregation: postAggregations) {
            updatedPostAggs.add(
                    replacePostAggregation(
                            SketchSetOperationPostAggFunction.INTERSECT,
                            postAggregation,
                            filteredAggDictionary
                    )
            );
        }

        //with the filtered aggregation and reconstructed post aggregation, generate new templateDruidQuery
        return new TemplateDruidQuery(updatedAggs, updatedPostAggs, query.getInnerQuery(), query.getTimeGrain());
    }

    @Override
    public PostAggregation replacePostAggregation(
            SketchSetOperationPostAggFunction func,
            PostAggregation postAggregation,
            Map<String, List<FilteredAggregation>> filteredAggDictionary
    ) {
        if (postAggregation instanceof WithFields) {
            WithFields withFieldsPostAgg = (WithFields) postAggregation;

            List<PostAggregation> resultPostAggsList = new ArrayList<>();
            //In case the postAgg has the function NOT, we apply INTERSECT on the left operand of the
            //function and UNION on the right operand of the function
            if (withFieldsPostAgg instanceof SketchSetOperationPostAggregation &&
                    ((SketchSetOperationPostAggregation) withFieldsPostAgg)
                            .getFunc()
                            .equals(SketchSetOperationPostAggFunction.NOT)) {
                SketchSetOperationPostAggregation sketchSetPostAgg =
                        (SketchSetOperationPostAggregation) withFieldsPostAgg;
                //INTERSECT for the left operand
                resultPostAggsList.add(
                        replacePostAggregation(
                                SketchSetOperationPostAggFunction.INTERSECT,
                                sketchSetPostAgg.getFields().get(0),
                                filteredAggDictionary
                        )
                );
                //UNION for the right operand
                resultPostAggsList.add(
                        replacePostAggregation(
                                SketchSetOperationPostAggFunction.UNION,
                                sketchSetPostAgg.getFields().get(1),
                                filteredAggDictionary
                        )
                );
                return withFieldsPostAgg.withFields(resultPostAggsList);
            }

            @SuppressWarnings("unchecked")
            List<PostAggregation> childPostAggs = withFieldsPostAgg.getFields();
            for (PostAggregation postAgg : childPostAggs) {
                resultPostAggsList.add(
                        replacePostAggregation(
                                SketchSetOperationPostAggFunction.INTERSECT,
                                postAgg,
                                filteredAggDictionary
                        )
                );
            }
            return withFieldsPostAgg.withFields(resultPostAggsList);

        } else if (postAggregation instanceof FieldAccessorPostAggregation) {
            //This postAgg is a leaf node i.e. a FieldAccessor
            //Lookup filteredAggregation from filteredAggDictionary and get a list of filteredAggregators for this
            //FieldAccessor. Apply the set operation 'func' on this list and generate postAgg.
            String fieldName = ((FieldAccessorPostAggregation) postAggregation).getFieldName();
            return SketchSetOperationHelper.makePostAggFromAgg(
                    func,
                    fieldName,
                    filteredAggDictionary.get(fieldName)
            );

        }  else {
            // Not an instance of WithField or of the type Constant
            return postAggregation;
        }
    }

   @Override
   public PostAggregation replacePostAggWithPostAggFromMap(
            PostAggregation postAggregation,
            Map<String, Aggregation> oldNameToNewAggregationMapping
    ) {
        if (postAggregation instanceof FieldAccessorPostAggregation) {

            //Check if the aggregation which this postAgg is referencing in the outerQuery has changed.
            //if so, create new FieldAccessor that accesses the changed aggregation
            String fieldName = ((FieldAccessorPostAggregation) postAggregation).getFieldName();
            if (oldNameToNewAggregationMapping.containsKey(fieldName)) {
                return new FieldAccessorPostAggregation(oldNameToNewAggregationMapping.get(fieldName));

            } else {
                //The agg which this fieldAccessor is referencing has not changed. So return the fieldAccessor as it is.
                return postAggregation;
            }

        } else if (postAggregation instanceof WithFields) {

            List<PostAggregation> resultPostAggsList = new ArrayList<>();
            @SuppressWarnings("unchecked")
            List<PostAggregation> childPostAggs = ((WithFields) postAggregation).getFields();
            for (PostAggregation postAgg : childPostAggs) {
                resultPostAggsList.add(replacePostAggWithPostAggFromMap(postAgg, oldNameToNewAggregationMapping));
            }

            return ((WithFields) postAggregation).withFields(resultPostAggsList);
        } else {
            return postAggregation;
        }
    }

    @Override
    public Set<FilteredAggregation> getFilteredAggregation(
            JsonNode filter,
            Aggregation aggregation,
            DimensionDictionary dimensionDictionary,
            LogicalTable table,
            DataApiRequest apiRequest
    ) throws DimensionRowNotFoundException {
        //Converting json filter string to a plain filter string to prepare the Filter out of it
        String filterString = filter.get("AND").asText().replace("],", "]],");
        String[] filterList = filterString.split("],");
        Set<FilteredAggregation> filteredAggregationSet = new HashSet<>();
        Map<String, Filter> filterHashMap = new HashMap<>();

        for (String aFilter : filterList) {
            Map<Dimension, Set<ApiFilter>> metricFilter = apiRequest.generateFilters(
                    aFilter,
                    table,
                    dimensionDictionary
            );
            filterHashMap.put(generateMetricName(aFilter), apiRequest.getFilterBuilder().buildFilters(metricFilter));
        }

        for (Map.Entry<String, Filter> entry: filterHashMap.entrySet()) {
            String newAggName = aggregation.getName().concat("-").concat(entry.getKey().toString());
            FilteredAggregation filteredAggregation = new FilteredAggregation(
                    newAggName,
                    aggregation,
                    filterHashMap.get(entry.getKey())
            );
            filteredAggregationSet.add(filteredAggregation);
        }
        return filteredAggregationSet;
    }

    @Override
    public String generateMetricName(String filterString) {
        return filterString.replace("|", "_").replace("-", "_").replace(",", "_").replace("]", "").replace("[", "_");
    }

    /**
     * Helper method that calls gatherFilterDimensions(Filter filter, {@literal Set<Dimension>} dimensions)
     * by passing in filter and an empty HashSet.
     *
     * @param filter  Filter object whose dimensions are to be collected
     *
     * @return Set of dimensions belonging to a Filter
     */
    @Override
    public Set<Dimension> gatherFilterDimensions(Filter filter) {
        return gatherFilterDimensions(filter, new HashSet<>());
    }

    @Override
    public Set<Dimension> gatherFilterDimensions(Filter filter, Set<Dimension> dimensions) {

        if (filter instanceof SelectorFilter) {
            dimensions.add(((SelectorFilter) filter).getDimension());
        } else if (filter instanceof MultiClauseFilter) {
            for (Filter multiclauseFilter: ((MultiClauseFilter) filter).getFields()) {
                gatherFilterDimensions(multiclauseFilter, dimensions);
            }
        } else if (filter instanceof NotFilter) {
            gatherFilterDimensions(((NotFilter) filter).getField(), dimensions);
        }
        return dimensions;
    }
}
