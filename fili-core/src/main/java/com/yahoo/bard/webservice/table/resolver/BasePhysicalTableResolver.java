// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.TableUtils;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.codahale.metrics.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/**
 *  Abstract parent to with business rule agnostic implementations of core methods
 */
public abstract class BasePhysicalTableResolver implements PhysicalTableResolver {

    private static final Logger LOG = LoggerFactory.getLogger(BasePhysicalTableResolver.class);
    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();

    BiFunction<DataApiRequest, TemplateDruidQuery, Granularity> resolveAcceptingGrain;

    public BasePhysicalTableResolver() {
        this.resolveAcceptingGrain = new RequestQueryGranularityResolver();
    }

    /**
     * Create a list of matchers based on a request and query
     *
     * @param apiRequest  The ApiRequest for the query
     * @param query a partial query representation
     *
     * @return a list of matchers to be applied, in order
     */
    public abstract List<PhysicalTableMatcher> getMatchers(DataApiRequest apiRequest, TemplateDruidQuery query);

    /**
     * Create a binary operator which returns the 'better' of two physical table
     *
     * @param apiRequest  The ApiRequest for the query
     * @param query a partial query representation
     *
     * @return a list of matchers to be applied, in order
     */
    public abstract BinaryOperator<PhysicalTable> getBetterTableOperator(
            DataApiRequest apiRequest,
            TemplateDruidQuery query
    );

    /**
     * Filter to a set of tables matching the rules of this resolver
     *
     * @param candidateTables  The physical tables being filtered
     * @param apiRequest  The request being filtered to
     * @param query  a partial query representation
     *
     * @return a set of physical tables which all match the criteria of a request and partial query
     *
     * @throws NoMatchFoundException if any of the filters reduce the filter set to empty
     */
    public Set<PhysicalTable> filter(
            Collection<PhysicalTable> candidateTables,
            DataApiRequest apiRequest,
            TemplateDruidQuery query
    ) throws NoMatchFoundException {
        return filter(candidateTables, getMatchers(apiRequest, query));
    }


    /**
     * Filter a list of tables through each matcher using a list of matchers sequentially
     *
     * @param candidateTables  The collection of tables to be filtered
     * @param matchers  The matchers to apply, in order
     *
     * @return A set of tables which satisfy all matchers
     *
     * @throws NoMatchFoundException if no tables match the filter
     */
    public Set<PhysicalTable> filter(
            Collection<PhysicalTable> candidateTables,
            List<PhysicalTableMatcher> matchers
    ) throws NoMatchFoundException {
        Collection<PhysicalTable> currentMatches = candidateTables;
        for (PhysicalTableMatcher matcher : matchers) {
            currentMatches = matcher.matchNotEmpty(currentMatches.stream());
        }
        return new LinkedHashSet<>(currentMatches);
    }

    @Override
    public PhysicalTable resolve(
            Collection<PhysicalTable> candidateTables,
            DataApiRequest apiRequest,
            TemplateDruidQuery query
    ) throws NoMatchFoundException {
        // Minimum grain at which the request can be aggregated from
        Granularity minimumTableTimeGrain = resolveAcceptingGrain.apply(apiRequest, query);
        TemplateDruidQuery innerQuery = (TemplateDruidQuery) query.getInnermostQuery();
        Set<String> columnNames = TableUtils.getColumnNames(apiRequest, innerQuery);
        LOG.trace(
                "Resolving Table using TimeGrain: {}, columns: {} and TableGroup: {}",
                minimumTableTimeGrain,
                columnNames,
                candidateTables
        );

        try {
            Set<PhysicalTable> physicalTables = filter(candidateTables, apiRequest, query);

            BinaryOperator<PhysicalTable> betterTable = getBetterTableOperator(apiRequest, query);
            PhysicalTable bestTable = physicalTables.stream().reduce(betterTable).get();

            REGISTRY.meter("request.physical.table." + bestTable.getName() + "." + bestTable.getTimeGrain()).mark();
            LOG.trace("Found best Table: {}", bestTable);
            return bestTable;
        } catch (NoMatchFoundException me) {
            // Blow up if we couldn't match a table, log and return if we can
            logMatchException(apiRequest, minimumTableTimeGrain, innerQuery, me);
            throw me;
        }
    }

    public void logMatchException (
            final DataApiRequest apiRequest,
            final Granularity minimumTableTimeGrain,
            final TemplateDruidQuery innerQuery,
            final NoMatchFoundException noMatchException
    ) throws NoMatchFoundException {
        // Get the dimensions and metrics as lists of names
        Set<String> requestDimensionNames = TableUtils.getDimensionColumnNames(apiRequest, innerQuery);

        Set<String> requestMetricNames = apiRequest.getLogicalMetrics().stream()
                .map(LogicalMetric::getName)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        String msg = ErrorMessageFormat.NO_PHYSICAL_TABLE_MATCHED.logFormat(
                requestDimensionNames,
                requestMetricNames,
                minimumTableTimeGrain
        );
        LOG.error(msg);
    }
}
