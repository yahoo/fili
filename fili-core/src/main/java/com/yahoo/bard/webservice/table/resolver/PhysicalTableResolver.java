// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.web.DataApiRequest;

import java.util.Collection;

/**
 * Physical table resolver selects the best physical table that satisfied a query (if any) from a supply of candidate
 * physical tables.
 */
public interface PhysicalTableResolver {

    /**
     * Choose the best fit Physical Table from a table group.
     *
     * @param candidateTables  The tables being considered for match
     * @param apiRequest  The ApiRequest for the query
     * @param query  a partial query representation
     *
     * @return The table, if any, that satisfies all criteria and best matches the query
     *
     * @throws NoMatchFoundException if there is no matching physical table in the table group
     */
    PhysicalTable resolve(
            Collection<PhysicalTable> candidateTables,
            DataApiRequest apiRequest,
            TemplateDruidQuery query
    ) throws NoMatchFoundException;
}
