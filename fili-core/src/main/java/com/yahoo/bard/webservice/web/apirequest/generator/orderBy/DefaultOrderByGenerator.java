// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.orderBy;

import static com.yahoo.bard.webservice.druid.model.orderby.OrderByColumnType.DIMENSION;
import static com.yahoo.bard.webservice.druid.model.orderby.OrderByColumnType.METRIC;
import static com.yahoo.bard.webservice.druid.model.orderby.OrderByColumnType.TIME;
import static com.yahoo.bard.webservice.druid.model.orderby.OrderByColumnType.UNKNOWN;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DATE_TIME_SORT_VALUE_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_DIRECTION_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_METRICS_NOT_IN_QUERY_FORMAT;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_METRICS_NOT_SORTABLE_FORMAT;
import static com.yahoo.bard.webservice.web.apirequest.DataApiRequest.DATE_TIME_STRING;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.generator.Generator;
import com.yahoo.bard.webservice.web.apirequest.generator.LegacyGenerator;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default generator implementation for binding logical metrics. Binding logical metrics is dependent on the logical
 * table being queried. Ensure the logical table has been bound before using this class to generate logical metrics.
 */
public class DefaultOrderByGenerator implements Generator<List<OrderByColumn>>, LegacyGenerator<List<OrderByColumn>> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultOrderByGenerator.class);

    @Override
    public List<OrderByColumn> bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        return generateBoundOrderByColumns(
                params.getSorts().orElse(null),
                builder.getLogicalMetricsIfInitialized(),
                builder.getDimensionsIfInitialized()
        );
    }

    @Override
    public void validate(
            final List<OrderByColumn> entity,
            final DataApiRequestBuilder builder,
            final RequestParameters params,
            final BardConfigResources resources
    ) {
        validate(
                entity,
                params.getSorts().orElse(null),
                builder.getLogicalMetricsIfInitialized(),
                builder.getDimensionsIfInitialized()
        );
    }

    @Override
    public List<OrderByColumn> bind(DataApiRequest request, String requestString, ResourceDictionaries dictionaries
    ) {
        return generateBoundOrderByColumns(
                requestString,
                request.getLogicalMetrics(),
                request.getDimensions()
        );
    }

    @Override
    public void validate(
            List<OrderByColumn> entity,
            DataApiRequest builder,
            String parameter,
            ResourceDictionaries dictionaries
    ) {
        validate(
                entity,
                parameter,
                builder.getLogicalMetrics(),
                builder.getDimensions()
        );
    }

    /**
     * Validate used by DataApiRequestImpl.
     *
     * @param orderByColumns  the columns generated
     * @param orderByRequest the original request text
     * @param selectedMetrics metrics selected in the request
     * @param selectedDimensions grouping dimensions selected in the request.  (not currently validating)
     */
    public void validate(
            List<OrderByColumn> orderByColumns,
            String orderByRequest,
            Set<LogicalMetric> selectedMetrics,
            Set<Dimension> selectedDimensions
    ) {
        // If any sort columns don't match a known selected column, fail the query
        List<String> unknownColumns = orderByColumns.stream()
                .filter(orderByColumn -> orderByColumn.getType().equals(UNKNOWN))
                .map(OrderByColumn::getDimension)
                .collect(Collectors.toList());

        String matchableMetrics = selectedMetrics.stream()
                .map(LogicalMetric::getName)
                .collect(Collectors.joining(",", "[", "]"));
        if (!unknownColumns.isEmpty()) {
            LOG.debug(SORT_METRICS_NOT_IN_QUERY_FORMAT.logFormat(unknownColumns, matchableMetrics));
            throw new BadApiRequestException(SORT_METRICS_NOT_IN_QUERY_FORMAT.format(unknownColumns, matchableMetrics));
        }

        // If any metrics being sorted on don't have a query column (an odd edge case, but it happens)
        // throw an error.
        Map<String, LogicalMetric> metricsByName = selectedMetrics.stream()
                .collect(Collectors.toMap(
                        LogicalMetric::getName,
                        x -> x
                ));

        List<String> noSortableColumn = orderByColumns.stream()
                .filter(orderByColumn -> orderByColumn.getType().equals(METRIC))
                .map(OrderByColumn::getDimension)
                .filter(name -> metricsByName.get(name).getTemplateDruidQuery() == null)
                .collect(Collectors.toList());

        if (!noSortableColumn.isEmpty()) {
            LOG.debug(SORT_METRICS_NOT_SORTABLE_FORMAT.logFormat(noSortableColumn.toString()));
            throw new BadApiRequestException(SORT_METRICS_NOT_SORTABLE_FORMAT.format(noSortableColumn));
        }

        // The system doesn't support sorting by datetime inside druid so all datetime sorting is done in
        // fili.  We currently only support sorting on datetime in fili, so it must be the first sorting criteria
        // or not at all.
        if (orderByColumns != null && orderByColumns.size() > 1) {
            if (orderByColumns.stream()
                    .skip(1)
                    .anyMatch(column -> column.getType().equals(TIME))) {
                LOG.debug(DATE_TIME_SORT_VALUE_INVALID.logFormat());
                throw new BadApiRequestException(DATE_TIME_SORT_VALUE_INVALID.format());
            }
        }

    }

    /**
     * Generates a Set of OrderByColumn associated with selected columns.
     *
     * These columns should be associated with one or more selected columns in the fact request.
     *
     * @param orderByRequest  api request string for the sort clause
     * @param selectedMetrics  Set of LogicalMetrics selected in the api request
     * @param selectedDimensions  Set of grouping dimensions selected in the api request
     *
     * @return a List of OrderByColumn whose names are physical column names or "dateTime"
     *
     * @throws BadApiRequestException if the sort clause is invalid due to unresolvable logical column names.
     */
    public List<OrderByColumn> generateBoundOrderByColumns(
            String orderByRequest,
            Set<LogicalMetric> selectedMetrics,
            Set<Dimension> selectedDimensions
    ) throws BadApiRequestException {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingSortColumns")) {


            // These requested order by columns are not bound to configuration entities
            List<OrderByColumn> rawColumns = parseOrderByColumns(orderByRequest);


            List<OrderByColumn> typeBoundColumns = new ArrayList<>();

            Map<String, LogicalMetric> metricsByName = selectedMetrics.stream()
                    .collect(Collectors.toMap(
                            LogicalMetric::getName,
                            x -> x
                    ));

            Map<String, Dimension> dimensionsByName = selectedDimensions.stream()
                    .collect(Collectors.toMap(
                            Dimension::getApiName,
                            x -> x
                    ));

            // Associate each column with a type determined using the column name
            for (OrderByColumn column : rawColumns) {
                OrderByColumn result = bindOrderByColumn(metricsByName, dimensionsByName, column);
                typeBoundColumns.add(result);
            }
            return typeBoundColumns;
        }
    }

    /**
     * Method to convert sort list to column and direction map.
     *
     * This results in unbound order by requests with no specified type.
     *
     * @param sortsRequest  String of sort columns
     *
     * @return LinkedHashMap of columns and their direction. Using LinkedHashMap to preserve the order
     */
    protected List<OrderByColumn> parseOrderByColumns(String sortsRequest) {
        if (sortsRequest == null || sortsRequest.isEmpty()) {
            return Collections.emptyList();
        }

        List<OrderByColumn> result = Arrays.stream(sortsRequest.split(","))
                .map(e -> Arrays.asList(e.split("\\|")))
                .map(e -> new OrderByColumn(e.get(0), parseSortDirection(e)))
                .collect(Collectors.toList());
        return result;
    }

    /**
     * Extract valid sort direction.
     *
     * @param columnWithDirection  Column and its sorting direction
     *
     * @return Sorting direction. If no direction provided then the default one will be DESC
     */
    protected SortDirection parseSortDirection(List<String> columnWithDirection) {
        if (columnWithDirection.size() < 2) {
            return SortDirection.DESC;
        }
        String normalized = columnWithDirection.get(1).toUpperCase(Locale.ENGLISH);
        Optional<SortDirection> direction = Arrays.<SortDirection>stream(SortDirection.values())
                .filter(s -> normalized.startsWith(s.name()))
                .findFirst();

        if (direction.isPresent()) {
            return direction.get();
        }

        String sortDirectionName = columnWithDirection.get(1);
        LOG.debug(SORT_DIRECTION_INVALID.logFormat(sortDirectionName));
        throw new BadApiRequestException(SORT_DIRECTION_INVALID.format(sortDirectionName));
    }

    /**
     * Bind a request order by column to a selected request column.
     *
     * @param metricsByName Metrics by column name
     * @param dimensionsByName  Dimensions by apiName
     * @param column The unbound parsed column.
     *
     * @return A column bound to Time, Metric, Dimension or Unknown if no columns match
     */
    protected OrderByColumn bindOrderByColumn(
            Map<String, LogicalMetric> metricsByName,
            Map<String, Dimension> dimensionsByName,
            OrderByColumn column
    ) {
        OrderByColumn result;
        if (column.getDimension().equals(DATE_TIME_STRING)) {
            result = column.withType(TIME);
        } else if (metricsByName.containsKey(column.getDimension())) {
            result = column.withType(METRIC);
        } else if (dimensionsByName.containsKey(column.getDimension())) {

            Dimension d = dimensionsByName.get(column.getDimension());
            String physicalName = d.getKey().getName();
            result = new OrderByColumn(physicalName, column.getDirection(), DIMENSION);
        } else {
            result = column;
            // This is probably an error, but we'll let validate sort it out
        }
        return result;
    }
}
