// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.table.PhysicalTable;

import java.util.Collection;

/**
 * Physical table resolver selects the best physical table that satisfied a query (if any) from a supply of candidate
 * physical tables.
 */
@FunctionalInterface
public interface PhysicalTableResolver {

    /**
     * Choose the best fit Physical Table from a table group.
     *
     * @param candidateTables  The tables being considered for match
     * @param requestConstraint  Contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     *
     * @return The table, if any, that satisfies all criteria and best matches the query
     *
     * @throws NoMatchFoundException if there is no matching physical table in the table group
     */
    PhysicalTable resolve(
            Collection<PhysicalTable> candidateTables,
            QueryPlanningConstraint requestConstraint
    ) throws NoMatchFoundException;
}
