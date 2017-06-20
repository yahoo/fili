// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.codahale.metrics.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 *  Abstract parent to with business rule agnostic implementations of core methods.
 */
public abstract class BasePhysicalTableResolver implements PhysicalTableResolver {

    private static final Logger LOG = LoggerFactory.getLogger(BasePhysicalTableResolver.class);
    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();

    /**
     * Create a list of matchers based on a request and query.
     *
     * @param requestConstraint contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     *
     * @return a list of matchers to be applied, in order
     */
    public abstract List<PhysicalTableMatcher> getMatchers(QueryPlanningConstraint requestConstraint);

    /**
     * Create a binary operator which returns the 'better' of two physical table.
     *
     * @param requestConstraint contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     *
     * @return a list of matchers to be applied, in order
     */
    public abstract BinaryOperator<PhysicalTable> getBetterTableOperator(QueryPlanningConstraint requestConstraint);

    /**
     * Filter to a set of tables matching the rules of this resolver.
     *
     * @param candidateTables  The physical tables being filtered
     * @param requestConstraint contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     *
     * @return a set of physical tables which all match the criteria of a request and partial query
     *
     * @throws NoMatchFoundException if any of the filters reduce the filter set to empty
     */
    public Set<PhysicalTable> filter(
            Collection<PhysicalTable> candidateTables,
            QueryPlanningConstraint requestConstraint
    ) throws NoMatchFoundException {
        return filter(candidateTables, getMatchers(requestConstraint));
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
            QueryPlanningConstraint requestConstraint
    ) throws NoMatchFoundException {

        // Minimum grain at which the request can be aggregated from
        LOG.trace(
                "Resolving Table using TimeGrain: {}, dimension API names: {} and TableGroup: {}",
                requestConstraint.getMinimumGranularity(),
                requestConstraint.getAllColumnNames(),
                candidateTables
        );

        try {
            PhysicalTable bestTable = filter(candidateTables, requestConstraint).stream()
                    .reduce(getBetterTableOperator(requestConstraint))
                    .get();

            REGISTRY.meter(
                    "request.physical.table." + bestTable.getName() + "." + bestTable.getSchema().getTimeGrain()
            ).mark();
            LOG.trace("Found best Table: {}", bestTable);
            return bestTable;
        } catch (NoMatchFoundException me) {
            // Blow up if we couldn't match a table, log and return if we can
            LOG.error(ErrorMessageFormat.NO_PHYSICAL_TABLE_MATCHED.logFormat(
                    requestConstraint.getAllDimensionNames(),
                    requestConstraint.getLogicalMetricNames(),
                    requestConstraint.getMinimumGranularity()
            ));
            throw me;
        }
    }
}
