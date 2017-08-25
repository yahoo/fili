// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.datasource.QueryDataSource;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.metadata.RequestedIntervalsFunction;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.base.AbstractPeriod;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Druid lookback query.
 */
public class LookbackQuery extends AbstractDruidAggregationQuery<LookbackQuery> {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Having having;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final LimitSpec limitSpec;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Collection<String> lookbackPrefixes;

    private final Collection<Period> lookbackOffsets;

    /**
     * Constructor.
     *
     * @param dataSource  DataSource for the query
     * @param granularity  Granularity for the query
     * @param filter  Filter for the query
     * @param aggregations  Aggregations for the query
     * @param postAggregations  Post-aggregation operation trees for the query
     * @param context  Query context
     * @param intervals  Query intervals
     * @param doFork  If the query should track it's forks or not
     * @param lookbackOffsets  Set of period offsets
     * @param lookbackPrefixes  Set of prefixes for the lookback queries (should match the lookbackOffsets)
     * @param having  Having clause to apply to the result query
     * @param limitSpec  Limit spec to apply to the result query
     */
    private LookbackQuery(
            DataSource dataSource,
            Granularity granularity,
            Filter filter,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            QueryContext context,
            boolean doFork,
            Collection<Period> lookbackOffsets,
            Collection<String> lookbackPrefixes,
            Having having,
            LimitSpec limitSpec
    ) {
        super(
                DefaultQueryType.LOOKBACK,
                dataSource,
                granularity,
                Collections.<Dimension>emptySet(),
                filter,
                aggregations,
                postAggregations,
                intervals,
                context,
                doFork
        );

        this.having = having;
        this.limitSpec = limitSpec;
        this.lookbackOffsets = lookbackOffsets != null ? new ArrayList<>(lookbackOffsets) : null;
        this.lookbackPrefixes = lookbackPrefixes != null ? new ArrayList<>(lookbackPrefixes) : null;
    }

    /**
     * Constructor.
     *
     * @param dataSource  DataSource for the query
     * @param postAggregations  Post-aggregation operation trees for the query
     * @param context  Query context
     * @param lookbackOffsets  Set of period offsets
     * @param lookbackPrefixes  Set of prefixes for the lookback queries (should match the lookbackOffsets)
     * @param having  Having clause to apply to the result query
     * @param limitSpec  Limit spec to apply to the result query
     */
    public LookbackQuery(
            DataSource dataSource,
            Collection<PostAggregation> postAggregations,
            QueryContext context,
            Collection<Period> lookbackOffsets,
            Collection<String> lookbackPrefixes,
            Having having,
            LimitSpec limitSpec
    ) {
        this(
                dataSource,
                null,
                null,
                Collections.<Aggregation>emptySet(),
                postAggregations,
                Collections.<Interval>emptySet(),
                context,
                false,
                lookbackOffsets,
                lookbackPrefixes,
                having,
                limitSpec
        );
    }

    @Override
    public Optional<? extends DruidAggregationQuery> getInnerQuery() {
        return (Optional<? extends DruidAggregationQuery>) this.dataSource.getQuery();
    }

    /**
     * Return the Inner Query without checking that it exists.
     *
     * @return the inner query.
     */
    @JsonIgnore
    private DruidAggregationQuery<?> getInnerQueryUnchecked() {
        return getInnerQuery().get();
    }

    public Having getHaving() {
        return having;
    }

    public LimitSpec getLimitSpec() {
        return limitSpec;
    }

    public Collection<String> getLookbackPrefixes() {
        return lookbackPrefixes;
    }

    @JsonIgnore
    public Collection<Period> getLookbackOffsets() {
        return lookbackOffsets;
    }

    /**
     * Get the collection of lookback offsets.
     *
     * @return  The collection of lookback offsets.
     */
    @JsonProperty(value = "lookbackOffsets")
    public List<String> getlookbackOffsets() {
        return lookbackOffsets.stream().map(AbstractPeriod::toString).collect(Collectors.toList());
    }

    @Override
    @JsonIgnore
    public Granularity getGranularity() {
        return getInnerQueryUnchecked().getGranularity();
    }

    @Override
    @JsonIgnore
    public Set<Aggregation> getAggregations() {
        return getInnerQueryUnchecked().getAggregations();
    }

    @Override
    @JsonIgnore
    public Filter getFilter() {
        return getInnerQueryUnchecked().getFilter();
    }

    @Override
    @JsonIgnore
    public List<Interval> getIntervals() {
        return getInnerQueryUnchecked().getIntervals();
    }

    @Override
    @JsonIgnore
    public Collection<Dimension> getDimensions() {
        return getInnerQueryUnchecked().getDimensions();
    }

    @JsonProperty(value = "postAggregations")
    public Collection<PostAggregation> getLookbackPostAggregations() {
        return new LinkedHashSet<>(postAggregations);
    }

    @Override
    @JsonIgnore
    public Collection<PostAggregation> getPostAggregations() {
        return Stream.concat(getInnerQueryUnchecked().getPostAggregations().stream(), postAggregations.stream())
                .collect(Collectors.toCollection(LinkedHashSet<PostAggregation>::new));
    }

    // CHECKSTYLE:OFF

    @Override
    public LookbackQuery withAggregations(Collection<Aggregation> aggregations) {
        return withDataSource(new QueryDataSource(getInnerQueryUnchecked().withAggregations(aggregations)));
    }

    @Override
    public LookbackQuery withPostAggregations(Collection<PostAggregation> postAggregations) {
        throw new UnsupportedOperationException(ErrorMessageFormat.UNSUPPORTED_LOOKBACKQUERY_OPERATION.format());
    }

    /**
     * Update the postAggregations of the nested inner query. The PostAggregations of the LookbackQuery(outer query)
     * remain unchanged
     *
     * @param postAggregations  A Collection of PostAggregations
     *
     * @return A LookbackQuery whose datasource is built using the provided postAggregations
     */
    public LookbackQuery withInnerQueryPostAggregations(Collection<PostAggregation> postAggregations) {
        return new LookbackQuery(new QueryDataSource(getInnerQueryUnchecked().withPostAggregations(postAggregations)), granularity, filter, aggregations, getLookbackPostAggregations(), intervals, context, false, lookbackOffsets, lookbackPrefixes, having, limitSpec);
    }

    /**
     * Update the postAggregations  of the LookbackQuery(outer query).
     *
     * @param postAggregations  A Collection of PostAggregations
     *
     * @return A LookbackQuery built using the provided postAggregations
     */
    public LookbackQuery withLookbackQueryPostAggregations(Collection<PostAggregation> postAggregations) {
        return new LookbackQuery(dataSource, granularity, filter, aggregations, postAggregations, intervals, context, false, lookbackOffsets, lookbackPrefixes, having, limitSpec);
    }

    @Override
    public LookbackQuery withGranularity(Granularity granularity) {
        return withDataSource(new QueryDataSource(getInnerQueryUnchecked().withGranularity(granularity)));
    }

    @Override
    public LookbackQuery withFilter(Filter filter) {
        return withDataSource(new QueryDataSource(getInnerQueryUnchecked().withFilter(filter)));
    }

    @Override
    public LookbackQuery withIntervals(Collection<Interval> intervals) {
        return withDataSource(new QueryDataSource(getInnerQueryUnchecked().withIntervals(intervals)));
    }

    @Override
    public LookbackQuery withAllIntervals(Collection<Interval> intervals) {
        Optional<DruidFactQuery<?>> innerQuery = (Optional<DruidFactQuery<?>>) this.dataSource.getQuery();
        return !innerQuery.isPresent() ?
                withIntervals(intervals) :
                withDataSource(new QueryDataSource(innerQuery.get().withAllIntervals(intervals))).withIntervals(intervals);
    }

    @Override
    public LookbackQuery withDataSource(DataSource dataSource) {
        return new LookbackQuery(dataSource, granularity, filter, aggregations, postAggregations, intervals, context, false, lookbackOffsets, lookbackPrefixes, having, limitSpec);
    }

    @Override
    public LookbackQuery withInnermostDataSource(DataSource dataSource) {
        Optional<DruidFactQuery<?>> innerQuery = (Optional<DruidFactQuery<?>>) this.dataSource.getQuery();
        return (innerQuery == null) ?
                withDataSource(dataSource) :
                withDataSource(new QueryDataSource(innerQuery.get().withInnermostDataSource(dataSource)));
    }

    @Override
    public LookbackQuery withContext(QueryContext context) {
        return new LookbackQuery(dataSource, granularity, filter, aggregations, postAggregations, intervals, context, false, lookbackOffsets, lookbackPrefixes, having, limitSpec);
    }

    public LookbackQuery withOrderBy(LimitSpec limitSpec) {
        return new LookbackQuery(dataSource, granularity, filter, aggregations, postAggregations, intervals, context, false, lookbackOffsets, lookbackPrefixes, having, limitSpec);
    }

    public LookbackQuery withHaving(Having having) {
        return new LookbackQuery(dataSource, granularity, filter, aggregations, postAggregations, intervals, context, false, lookbackOffsets, lookbackPrefixes, having, limitSpec);
    }

    public LookbackQuery withLookbackPrefix(List<String> lookbackPrefixes) {
        return new LookbackQuery(dataSource, granularity, filter, aggregations, postAggregations, intervals, context, false, lookbackOffsets, lookbackPrefixes, having, limitSpec);
    }

    public LookbackQuery withLookbackOffsets(List<Period> lookbackOffsets) {
        return new LookbackQuery(dataSource, granularity, filter, aggregations, postAggregations, intervals, context, false, lookbackOffsets, lookbackPrefixes, having, limitSpec);
    }
    // CHECKSTYLE:ON

    /**
     * The function to get requestedIntervals from a LookbackQuery.
     */
    public static class LookbackQueryRequestedIntervalsFunction implements RequestedIntervalsFunction {

        @Override
        public SimplifiedIntervalList apply(DruidAggregationQuery<?> druidAggregationQuery) {

            LookbackQuery castQuery = (LookbackQuery) druidAggregationQuery;
            return Stream.concat(
                    castQuery.getIntervals().stream(),
                    castQuery.getIntervals().stream()
                            .flatMap(interval -> castQuery.getLookbackOffsets().stream()
                                    .map(lookbackOffset -> getCohortInterval(interval, lookbackOffset))
                            )
            ).collect(SimplifiedIntervalList.getCollector());
        }

        /**
         * Given an interval and a lookbackOffset, calculate the cohortInterval.
         *
         * @param interval  The measurement interval to which the offset is added
         * @param lookbackOffset  The offset to be added to the given interval
         *
         * @return Cohort interval calculated using the given interval and lookbackOffset
         */
        private Interval getCohortInterval(Interval interval, Period lookbackOffset) {
            return new Interval(
                    interval.getStart().withPeriodAdded(lookbackOffset, 1),
                    interval.getEnd().withPeriodAdded(lookbackOffset, 1)
            );
        }
    }
}
