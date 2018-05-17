// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.CountAggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.SketchAggregation;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.datasource.QueryDataSource;
import com.yahoo.bard.webservice.druid.model.datasource.UnionDataSource;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec;
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Query to generate weight to evaluate the query.
 * <p>
 * Calculates # of sketches times the number rows which will be in result set <pre><code>
 *  {
 *    "queryType": "groupBy",
 *    "dataSource": {
 *      "query": {
 *        "queryType": "groupBy",
 *        "dataSource": {
 *          "name": "basefact_network",
 *          "type": "table"
 *        },
 *        "dimensions": [
 *          "user_device_type",
 *          "pty_country",
 *          "device_type_id"
 *        ],
 *        "aggregations": [
 *          {
 *            "name": "ignored",
 *            "type": "count"
 *          }
 *        ],
 *        "postAggregations": [
 *          {
 *            "type": "constant",
 *            "name": "count",
 *            "value": 2
 *          }
 *        ],
 *        "intervals": [
 *          "2014-09-01T00:00:00.000Z/2014-09-30T00:00:00.000Z"
 *        ],
 *        "granularity": {
 *          "type": "period",
 *          "period": "P1D"
 *        }
 *      },
 *      "type": "query"
 *    },
 *    "dimensions": [],
 *    "aggregations": [
 *      {
 *        "name": "count",
 *        "fieldName": "count",
 *        "type": "longSum"
 *      }
 *    ],
 *    "postAggregations": [],
 *    "intervals": [
 *      "2014-09-01T00:00:00.000Z/2014-09-30T00:00:00.000Z"
 *    ],
 *    "granularity": "all"
 *  }
 * </code></pre>
 */
public class WeightEvaluationQuery extends GroupByQuery {
    private static final Logger LOG = LoggerFactory.getLogger(WeightEvaluationQuery.class);
    public static final long DEFAULT_DRUID_TOP_N_THRESHOLD = 1000;

    /**
     * Generate a query that calculates the even weight of the response cardinality of the given query.
     *
     * @param query  Query to calculate the weighted response cardinality of
     * @param weight  Weight to apply to each response row
     */
    public WeightEvaluationQuery(DruidAggregationQuery<?> query, int weight) {
        super(
                makeInnerQuery(query, weight),
                AllGranularity.INSTANCE,
                Collections.<Dimension>emptyList(),
                (Filter) null,
                (Having) null,
                Collections.<Aggregation>singletonList(new LongSumAggregation("count", "count")),
                Collections.<PostAggregation>emptyList(),
                query.getIntervals(),
                query.getQueryType() == DefaultQueryType.GROUP_BY ? stripColumnsFromLimitSpec(query) : null
        );
    }

    // Instead of TimeGrain, this query uses granularity: all
    @Override
    public Granularity getGranularity() {
        return AllGranularity.INSTANCE;
    }

    /**
     * Evaluate Druid query for expensive aggregation that could bring down Druid.
     *
     * @param query  Druid Query
     *
     * @return query or null if not required
     */
    public static WeightEvaluationQuery makeWeightEvaluationQuery(DruidAggregationQuery<?> query) {
        // get inner-most query for evaluation
        DruidAggregationQuery<?> innerQuery = query.getInnermostQuery();

        int weight = Utils.getSubsetByType(innerQuery.getAggregations(), SketchAggregation.class).size();

        return new WeightEvaluationQuery(innerQuery, weight);
    }

    /**
     * Evaluate Druid query for worst possible case expensive aggregation that could bring down Druid.
     * <p>
     * Number of Sketches * # of periods in iteration * cardinality of each dimension values
     *
     * @param query  The base query being estimated
     *
     * @return worst case rows
     * @throws ArithmeticException if the estimate is larger than {@link Long#MAX_VALUE}
     */
    public static long getWorstCaseWeightEstimate(DruidAggregationQuery<?> query) {
        DruidAggregationQuery<?> innerQuery = query.getInnermostQuery();

        int sketchWeight = Utils.getSubsetByType(innerQuery.getAggregations(), SketchAggregation.class).size();
        if (sketchWeight == 0) {
            return 0;
        }

        long periods = IntervalUtils.countSlicedIntervals(innerQuery.getIntervals(), innerQuery.getGranularity());
        long cardinalityWeight;


        if (innerQuery.getQueryType() == DefaultQueryType.TOP_N) {
            TopNQuery topNQuery = (TopNQuery) innerQuery;
            cardinalityWeight = Math.min(
                    topNQuery.getDimension().getCardinality(),
                    Math.max(topNQuery.getThreshold(), DEFAULT_DRUID_TOP_N_THRESHOLD)
            );
        } else {
            cardinalityWeight = innerQuery.getDimensions().stream()
                .mapToLong(Dimension::getCardinality)
                .filter(cardinality -> cardinality > 0)
                .reduce(1, Math::multiplyExact);
        }

        long weight = Math.multiplyExact(cardinalityWeight, Math.multiplyExact(sketchWeight, periods));
        LOG.debug("worst case weight = {}", weight);

        return weight;
    }

    /**
     * Make the inner query for the weight evaluation query. The primary point of the inner query is to provide a weight
     * per expected response row of the given query.
     *
     * @param query  Query to generate a weight query for
     * @param weight  Weight to apply to each row for the weight query
     *
     * @return A weight query that gives a weight per expected response row of the given query
     */
    private static DataSource makeInnerQuery(DruidAggregationQuery<?> query, double weight) {
        DruidAggregationQuery<?> innerQuery = query.getInnermostQuery();

        List<Aggregation> aggregations;
        aggregations = Collections.singletonList(new CountAggregation("ignored"));

        // Get the inner post aggregation
        List<PostAggregation> postAggregations;
        postAggregations = Collections.singletonList(new ConstantPostAggregation("count", weight));

        if (!(innerQuery.getQueryType() instanceof DefaultQueryType)) {
            return null;
        }

        DefaultQueryType innerQueryType = (DefaultQueryType) innerQuery.getQueryType();
        switch (innerQueryType) {
            case GROUP_BY:
                GroupByQuery inner = new GroupByQuery(
                        innerQuery.getDataSource(),
                        innerQuery.getGranularity(),
                        innerQuery.getDimensions(),
                        innerQuery.getFilter(),
                        (Having) null,
                        aggregations,
                        postAggregations,
                        innerQuery.getIntervals(),
                        stripColumnsFromLimitSpec(innerQuery)
                );
                return new QueryDataSource(inner);
            case TOP_N:
                TopNQuery topNQuery = (TopNQuery) innerQuery;
                GroupByQuery transformed = new GroupByQuery(
                        new UnionDataSource(topNQuery.getDataSource().getPhysicalTable()),
                        topNQuery.getGranularity(),
                        topNQuery.getDimensions(),
                        topNQuery.getFilter(),
                        null,
                        aggregations,
                        postAggregations,
                        topNQuery.getIntervals(),
                        null
                );
                return new QueryDataSource(transformed);
            default:
                return null;
        }
    }

    /**
     * Strip the columns from the LimitSpec on the query and return it, if present.
     *
     * @param query  Query to strip the columns from within the LimitSpec
     *
     * @return the cleaned LimitSpec if there is one
     */
    private static LimitSpec stripColumnsFromLimitSpec(DruidFactQuery query) {
        return ((GroupByQuery) query).getLimitSpec() == null ?
                null :
                ((GroupByQuery) query).getLimitSpec().withColumns(new LinkedHashSet<>());
    }
}
