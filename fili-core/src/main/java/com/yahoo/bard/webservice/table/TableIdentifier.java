// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.web.DataApiRequest;

import org.joda.time.ReadablePeriod;

import java.util.AbstractMap;
import java.util.Optional;

/**
 * Identifies a table by it's logical name and time grain.
 */
public class TableIdentifier extends AbstractMap.SimpleImmutableEntry<String, Optional<ReadablePeriod>> {

    /**
     * Constructor.
     *
     * @param logicalTableName  Name for the table identifier
     * @param granularity  Granularity of the table identifier
     */
    public TableIdentifier(String logicalTableName, Granularity granularity) {
        super(logicalTableName, getGranularityPeriod(granularity));
    }

    /**
     * Constructor.
     *
     * @param logicalTableName  Name for the table identifier
     * @param period  Period for the table identifier
     */
    public TableIdentifier(String logicalTableName, Optional<ReadablePeriod> period) {
        super(logicalTableName, period);
    }

    /**
     * Constructor.
     *
     * @param table  Logical table for the table identifier
     */
    public TableIdentifier(LogicalTable table) {
        this(table.getName(), table.getGranularity());
    }

    /**
     * Builder.
     *
     * @param request  API Request for which to build a table identifier
     *
     * @return the table identifier for the request
     */
    public static TableIdentifier create(DataApiRequest request) {
        return new TableIdentifier(request.getTable().getName(), getGranularityPeriod(request.getGranularity()));
    }

    /**
     * Extract the period from the granularity.
     *
     * @param granularity  Granularity to extract the period from
     *
     * @return an Optional with the period if the granularity had one, or empty otherwise.
     */
    private static Optional<ReadablePeriod> getGranularityPeriod(Granularity granularity) {
        return Optional.ofNullable(
                granularity instanceof TimeGrain ? ((TimeGrain) granularity).getPeriod() : null
        );
    }
}
