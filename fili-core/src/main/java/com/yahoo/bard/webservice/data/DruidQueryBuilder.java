// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TOP_N_UNSORTED;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionRowNotFoundException;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.datasource.QueryDataSource;
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource;
import com.yahoo.bard.webservice.druid.model.datasource.UnionDataSource;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.druid.model.orderby.TopNMetric;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery;
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery;
import com.yahoo.bard.webservice.druid.model.query.TopNQuery;
import com.yahoo.bard.webservice.table.ConstrainedTable;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.table.resolver.NoMatchFoundException;
import com.yahoo.bard.webservice.table.resolver.PhysicalTableResolver;
import com.yahoo.bard.webservice.table.resolver.QueryPlanningConstraint;
import com.yahoo.bard.webservice.web.DataApiRequest;

import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Druid Query builder class.
 */
@Singleton
public class DruidQueryBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(DruidQueryBuilder.class);
    protected final LogicalTableDictionary tableDictionary;
    protected final PhysicalTableResolver resolver;

    /**
     * Constructor.
     *
     * @param tableDictionary  Dictionary of logical tables used to look up table groups
     * @param resolver  Strategy for resolving the physical table
     */
    @Inject
    public DruidQueryBuilder(LogicalTableDictionary tableDictionary, PhysicalTableResolver resolver) {
        this.tableDictionary = tableDictionary;
        this.resolver = resolver;
        LOG.trace("Table dictionary: {} \nPhysical table resolver: {}", tableDictionary, resolver);
    }

    /**
     * Build a druid query object from an API request and it's templateDruidQuery.
     *
     * @param request  DataApiRequest to use in building the query
     * @param template  TemplateDruidQuery to build out the query with
     *
     * @return a DruidAggregationQuery
     * @throws DimensionRowNotFoundException if the filters filter out all dimension rows
     * @throws NoMatchFoundException if no PhysicalTable satisfies this request
     */
    public DruidAggregationQuery<?> buildQuery(
            DataApiRequest request,
            TemplateDruidQuery template
    ) throws DimensionRowNotFoundException, NoMatchFoundException {

        LOG.trace("Building druid query with DataApiRequest: {} and TemplateDruidQuery: {}", request, template);

        // Whether to build the orderBy clause
        LimitSpec druidOrderBy;
        // Whether to build a topN query
        TopNMetric druidTopNMetric;

        if (request.getTopN().isPresent()) {
            //This is a topN query

            if (canOptimizeTopN(request, template)) {
                druidOrderBy = null;
                OrderByColumn sortBy = request.getSorts().iterator().next();
                druidTopNMetric = new TopNMetric(sortBy.getDimension(), sortBy.getDirection());
            } else if (request.getSorts().size() > 0) {
                druidOrderBy = new LimitSpec(request.getSorts());
                druidTopNMetric = null;
            } else {
                // We don't expect to reach this point. This is checked in DataApiRequest. Here for completeness
                throw new UnsupportedOperationException(TOP_N_UNSORTED.format(request.getTopN()));
            }
        } else if (request.getSorts().isEmpty() && !request.getCount().isPresent()) {
            //This is an arbitrary groupBy query
            druidOrderBy = null;
            druidTopNMetric = null;
        } else {
            //This is a sorted and/or a limited groupBy query
            druidOrderBy = new LimitSpec(request.getSorts(), request.getCount());
            druidTopNMetric = null;
        }

        // Get the tableGroup from the logical table and the alias
        LogicalTable logicalTable = tableDictionary.get(TableIdentifier.create(request));
        TableGroup group = logicalTable.getTableGroup();

        // Resolve the table from the the group, the combined dimensions in request, and template time grain
        QueryPlanningConstraint constraint = new QueryPlanningConstraint(request, template);
        ConstrainedTable table = resolver.resolve(group.getPhysicalTables(), constraint).withConstraint(constraint);

        return druidTopNMetric != null ?
            buildTopNQuery(
                    template,
                    table,
                    request.getGranularity(),
                    request.getTimeZone(),
                    request.getDimensions(),
                    request.getFilter(),
                    request.getIntervals(),
                    druidTopNMetric,
                    request.getTopN().getAsInt()
            ) :
             canOptimizeTimeSeries(request, template) ?
                buildTimeSeriesQuery(
                        template,
                        table,
                        request.getGranularity(),
                        request.getTimeZone(),
                        request.getFilter(),
                        request.getIntervals()
                ) :
                buildGroupByQuery(
                        template,
                        table,
                        request.getGranularity(),
                        request.getTimeZone(),
                        request.getDimensions(),
                        request.getFilter(),
                        request.getHaving(),
                        request.getIntervals(),
                        druidOrderBy
                );
    }

    /**
     * Builds a druid groupBy query recursively nesting dataSource based on the TemplateDruidQuery.
     *
     * @param template  The query template, possibly nested
     * @param table  The physical table that underlies the lowest-level datasource
     * @param granularity  The granularity from the request
     * @param timeZone  The time zone from the request
     * @param groupByDimensions  The grouping dimensions from the request
     * @param filter  The filters specified in the request (only applied at the lowest level)
     * @param having  The having clause specified in the request.
     * @param intervals  The intervals specified from the request
     * @param druidOrderBy  The order by
     *
     * @return a GroupByQuery
     */
    protected GroupByQuery buildGroupByQuery(
            TemplateDruidQuery template,
            ConstrainedTable table,
            Granularity granularity,
            DateTimeZone timeZone,
            Set<Dimension> groupByDimensions,
            Filter filter,
            Having having,
            Set<Interval> intervals,
            LimitSpec druidOrderBy
    ) {
        LOG.trace(
                "Building druid groupBy query with following parameters \n" +
                        "TemplateDruidQuery: {} \n" +
                        "TimeGrain: {} \n" +
                        "TimeZone: {} \n" +
                        "Table: {} \n" +
                        "Group by dimensions: {} \n" +
                        "Filter: {} \n" +
                        "Intervals: {} \n",
                template,
                granularity,
                timeZone,
                table,
                groupByDimensions,
                filter,
                intervals
        );

        Filter mergedFilter = filter;

        // Override the grain with what's set in the template if it has one set
        Granularity mergedGranularity = template.getTimeGrain() != null
                ? template.getTimeGrain().buildZonedTimeGrain(timeZone)
                : granularity;

        DataSource dataSource;
        if (!template.isNested()) {
            LOG.trace("Building a single pass druid groupBy query");
            dataSource = buildTableDataSource(table);
        } else {
            LOG.trace("Building a multi pass druid groupBy query");
            // Build the inner query without an order by, since we only want to do that at the top level
            // Sorts don't apply to inner queries and Filters only apply to the innermost query
            GroupByQuery query = buildGroupByQuery(
                    template.getInnerQuery(),
                    table,
                    mergedGranularity,
                    timeZone,
                    groupByDimensions,
                    mergedFilter,
                    having,
                    intervals,
                    (LimitSpec) null
            );
            dataSource = new QueryDataSource(query);
            // Filters have been handled by the inner query, are not needed/allowed on the outer query
            mergedFilter = null;
        }

        // Filters must be applied at the lowest level as they exclude data from aggregates
        return new GroupByQuery(
                dataSource,
                mergedGranularity,
                groupByDimensions,
                mergedFilter,
                having,
                template.getAggregations(),
                template.getPostAggregations(),
                intervals,
                druidOrderBy
        );
    }

    /**
     * Build a data source from a table.
     *
     * @param table A fact table or fact table view
     *
     * @return A table datasource for a fact table or a union data source for a fact table view
     */
    private DataSource buildTableDataSource(ConstrainedTable table) {
        if (table.getDataSourceNames().size() == 1) {
            return new TableDataSource(table);
        } else {
            return new UnionDataSource(table);
        }
    }

    /**
     * Builds a druid topN query.
     *
     * @param template  The query template. Not nested since nesting is not supported in druid topN queries
     * @param table  The physical table that underlies the lowest-level datasource
     * @param granularity  The grain from the request
     * @param timeZone  The time zone from the request
     * @param groupByDimension  The grouping dimension from the request
     * @param filter  The filters specified in the request
     * @param intervals  The intervals specified from the request
     * @param metricSpec The topn metric spec
     * @param topN  The number of requested top entries per time bucket
     *
     * @return a TopNQuery
     */
    protected TopNQuery buildTopNQuery(
            TemplateDruidQuery template,
            ConstrainedTable table,
            Granularity granularity,
            DateTimeZone timeZone,
            Set<Dimension> groupByDimension,
            Filter filter,
            Set<Interval> intervals,
            TopNMetric metricSpec,
            int topN
    ) {
        LOG.trace(
                "Building druid topN query with following parameters \n" +
                        "TemplateDruidQuery: {} \n" +
                        "TimeGrain: {} \n" +
                        "TimeZone: {} \n" +
                        "Table: {} \n" +
                        "Group by dimensions: {} \n" +
                        "Filter: {} \n" +
                        "Intervals: {} \n" +
                        "MetricSpec: {} \n" +
                        "Threshold: {} \n",
                template,
                granularity,
                timeZone,
                table,
                groupByDimension,
                filter,
                intervals,
                metricSpec,
                topN
        );

        // Override the grain with what's set in the template if it has one set
        Granularity mergeGrain = (template.getTimeGrain() != null) ?
                template.getTimeGrain().buildZonedTimeGrain(timeZone) :
                granularity;

        LOG.trace("Building a single pass druid topN query");

        // The data source is the table directly, since there is no nested query below us
        return new TopNQuery(
                buildTableDataSource(table),
                // The check that the set of dimensions has exactly one element is currently done above
                mergeGrain,
                groupByDimension.iterator().next(),
                filter,
                template.getAggregations(),
                template.getPostAggregations(),
                intervals,
                topN,
                metricSpec
        );
    }

    /**
     * Builds a druid TimeSeries query.
     *
     * @param template  The query template. Not nested since nesting is not supported in druid timeseries queries
     * @param table  The physical table that underlies the lowest-level datasource
     * @param granularity  The grain from the request
     * @param timeZone  The time zone from the request
     * @param filter  The filters specified in the request
     * @param intervals  The intervals specified from the request
     *
     * @return a TimeSeriesQuery
     */
    protected TimeSeriesQuery buildTimeSeriesQuery(
            TemplateDruidQuery template,
            ConstrainedTable table,
            Granularity granularity,
            DateTimeZone timeZone,
            Filter filter,
            Set<Interval> intervals
    ) {
        LOG.trace(
                "Building druid timeseries query with following parameters \n" +
                        "TemplateDruidQuery: {} \n" +
                        "TimeGrain: {} \n" +
                        "Time Zone: {} \n" +
                        "Table: {} \n" +
                        "Filter: {} \n" +
                        "Intervals: {} \n",
                template,
                granularity,
                timeZone,
                table,
                filter,
                intervals
        );

        // Override the grain with what's set in the template if it has one set
        if (template.getTimeGrain() != null) {
            granularity = template.getTimeGrain().buildZonedTimeGrain(timeZone);
        }

        LOG.trace("Building a single pass druid timeseries query");

        return new TimeSeriesQuery(
                buildTableDataSource(table),
                granularity,
                filter,
                template.getAggregations(),
                template.getPostAggregations(),
                intervals
        );
    }

    /**
     * Determine if the optimization to a TopN query can be done.
     *
     * @param apiRequest  The request data
     * @param templateDruidQuery  The template query
     *
     * @return true if the optimization can be done, false if it can't
     */
    protected boolean canOptimizeTopN(DataApiRequest apiRequest, TemplateDruidQuery templateDruidQuery) {
        return apiRequest.getDimensions().size() == 1 &&
                apiRequest.getSorts().size() == 1 &&
                !templateDruidQuery.isNested() &&
                BardFeatureFlag.TOP_N.isOn() &&
                apiRequest.getHaving() == null;
    }

    /**
     * Determine if the optimization to a Timeseries query can be done.
     *
     * @param apiRequest  The request data
     * @param templateDruidQuery  The template query
     *
     * @return true if the optimization can be done, false if it can't
     */
    protected boolean canOptimizeTimeSeries(DataApiRequest apiRequest, TemplateDruidQuery templateDruidQuery) {
        return apiRequest.getDimensions().isEmpty() &&
                !templateDruidQuery.isNested() &&
                apiRequest.getSorts().isEmpty() &&
                !apiRequest.getCount().isPresent() &&
                apiRequest.getHaving() == null;
    }
}
