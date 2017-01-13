// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.rfc.table;

import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.Column;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

public class PhysicalTableSchema extends LinkedHashSet<Column> implements GranularSchema {

    private static final Logger LOG = LoggerFactory.getLogger(PhysicalTableSchema.class);

    ZonedTimeGrain granularity;
    private final Map<String, String> logicalToPhysicalColumnNames;
    private final Map<String, Set<String>> physicalToLogicalColumnNames;

    public PhysicalTableSchema(
            Set<Column> columns,
            @NotNull ZonedTimeGrain timeGrain
    ) {
        this(columns, timeGrain, Collections.emptyMap());
    }

    public PhysicalTableSchema(
            Set<Column> columns,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Map<String, String> logicalToPhysicalColumnNames
    ) {
        this.granularity = timeGrain;

        this.logicalToPhysicalColumnNames = Collections.unmodifiableMap(logicalToPhysicalColumnNames);
        this.physicalToLogicalColumnNames = Collections.unmodifiableMap(
                this.logicalToPhysicalColumnNames.entrySet().stream().collect(
                        Collectors.groupingBy(
                                Map.Entry::getValue,
                                Collectors.mapping(Map.Entry::getKey, Collectors.toSet())
                        )
                )
        );
        addAll(columns);
    }

    /**
     * Translate a logical name into a physical column name. If no translation exists (i.e. they are the same),
     * then the logical name is returned.
     * <p>
     * NOTE: This defaulting behavior <em>WILL BE REMOVED</em> in future releases.
     * <p>
     * The defaulting behavior shouldn't be hit for Dimensions that are serialized via the default serializer and are
     * not properly configured with a logical-to-physical name mapping. Dimensions that are not "normal" dimensions,
     * such as dimensions used for DimensionSpecs in queries to do mapping from fact-level dimensions to something else,
     * should likely use their own serialization strategy so as to not hit this defaulting behavior.
     *
     * @param logicalName  Logical name to lookup in physical table
     * @return Translated logicalName if applicable
     */
    public String getPhysicalColumnName(String logicalName) {
        return logicalToPhysicalColumnNames.getOrDefault(logicalName, logicalName);
    }

    /**
     * Translate a physical name into a logical column name. If no translation exists (i.e. they are the same),
     * then the physical name is returned.
     *
     * @param physicalName  Physical name to lookup in physical table
     * @return Translated physicalName if applicable
     */
    public Set<String> getLogicalColumnNames(String physicalName) {
        return physicalToLogicalColumnNames.getOrDefault(physicalName, Collections.singleton(physicalName));
    }

    public boolean containsLogicalName(String logicalName) {
        return logicalToPhysicalColumnNames.containsKey(logicalName);
    }

    @Override
    public ZonedTimeGrain getGranularity() {
        return granularity;
    }
}
