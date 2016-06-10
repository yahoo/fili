// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.web.DataApiRequest;

import org.joda.time.ReadablePeriod;

import java.util.AbstractMap;
import java.util.Optional;

/**
 * Identifies a table by it's logical name and time grain
 */
public class TableIdentifier extends AbstractMap.SimpleImmutableEntry<String, Optional<ReadablePeriod>> {

    public TableIdentifier(String logicalTableName, Granularity granularity) {
        super(logicalTableName, getGranularityPeriod(granularity));
    }

    public TableIdentifier(String logicalTableName, Optional<ReadablePeriod> period) {
        super(logicalTableName, period);
    }

    public TableIdentifier(Table table) {
        this(table.getName(), table.getGranularity());
    }

    public static TableIdentifier create(DataApiRequest request) {
        return new TableIdentifier(request.getTable().getName(), getGranularityPeriod(request.getGranularity()));
    }

    // Short lived code, remove once TableIdentifiers no longer need periods
    public static Optional<ReadablePeriod> getGranularityPeriod(Granularity granularity) {
        return Optional.ofNullable(
                granularity instanceof TimeGrain ? ((TimeGrain) granularity).getPeriod() : null
        );
    }
}
