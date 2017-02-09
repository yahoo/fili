// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.PhysicalTable;
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

/**
 *  Abstract parent to with business rule agnostic implementations of core methods.
 */
public abstract class BasePhysicalTableResolver implements PhysicalTableResolver {

    private static final Logger LOG = LoggerFactory.getLogger(BasePhysicalTableResolver.class);
    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();

    protected final BiFunction<DataApiRequest, TemplateDruidQuery, Granularity> resolveAcceptingGrain;

    /**
     * Constructor.
     */
    public BasePhysicalTableResolver() {
        this.resolveAcceptingGrain = new RequestQueryGranularityResolver();
    }

    /**
     * Create a list of matchers based on a request and query.
     *
     * @param requestConstraints contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     *
     * @return a list of matchers to be applied, in order
     */
    public abstract List<PhysicalTableMatcher> getMatchers(QueryPlanningConstraint requestConstraints);

    /**
     * Create a binary operator which returns the 'better' of two physical table.
     *
     * @param requestConstraints contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     *
     * @return a list of matchers to be applied, in order
     */
    public abstract BinaryOperator<PhysicalTable> getBetterTableOperator(QueryPlanningConstraint requestConstraints);

    /**
     * Filter to a set of tables matching the rules of this resolver.
     *
     * @param candidateTables  The physical tables being filtered
     * @param requestConstraints contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     *
     * @return a set of physical tables which all match the criteria of a request and partial query
     *
     * @throws NoMatchFoundException if any of the filters reduce the filter set to empty
     */
    public Set<PhysicalTable> filter(
            Collection<PhysicalTable> candidateTables,
            QueryPlanningConstraint requestConstraints
    ) throws NoMatchFoundException {
        return filter(candidateTables, getMatchers(requestConstraints));
    }

    /**
     * Filter a list of tables through each matcher using a list of matchers sequentially.
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
            QueryPlanningConstraint requestConstraints
    ) throws NoMatchFoundException {

        // Minimum grain at which the request can be aggregated from
        Granularity minimumGranularity = requestConstraints.getMinimumGranularity();
        Set<String> columnNames = requestConstraints.getAllColumnNames();
        LOG.trace(
                "Resolving Table using TimeGrain: {}, dimension API names: {} and TableGroup: {}",
                minimumGranularity,
                columnNames,
                candidateTables
        );

        try {
            Set<PhysicalTable> physicalTables = filter(candidateTables, requestConstraints);

            BinaryOperator<PhysicalTable> betterTable = getBetterTableOperator(requestConstraints);
            PhysicalTable bestTable = physicalTables.stream().reduce(betterTable).get();

            REGISTRY.meter("request.physical.table." + bestTable.getName() + "." + bestTable.getTimeGrain()).mark();
            LOG.trace("Found best Table: {}", bestTable);
            return bestTable;
        } catch (NoMatchFoundException me) {
            // Blow up if we couldn't match a table, log and return if we can
            logMatchException(requestConstraints, minimumGranularity);
            throw me;
        }
    }

    /**
     * Log out inability to find a matching table.
     *
     * @param requestConstraints contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     * @param minimumTableTimeGrain  Minimum grain that we needed to meet
     */
    public void logMatchException(
            QueryPlanningConstraint requestConstraints,
            Granularity minimumTableTimeGrain
    ) {
        // Get the dimensions and metrics as lists of names
        Set<String> requestDimensionNames = requestConstraints.getAllDimensionNames();
        Set<String> requestMetricNames = requestConstraints.getLogicalMetricNames();

        String msg = ErrorMessageFormat.NO_PHYSICAL_TABLE_MATCHED.logFormat(
                requestDimensionNames,
                requestMetricNames,
                minimumTableTimeGrain
        );
        LOG.error(msg);
    }
}
