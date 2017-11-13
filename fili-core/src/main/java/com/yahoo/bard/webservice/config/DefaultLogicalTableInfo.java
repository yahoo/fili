// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

import org.joda.time.ReadablePeriod;
import org.joda.time.Years;

import java.util.Optional;

/**
 * Default for LogicalTableInfo interface, everything just returns getName.
 */
public class DefaultLogicalTableInfo implements LogicalTableInfo {

    public static final String DEFAULT_CATEGORY = "General";
    public static final ReadablePeriod DEFAULT_RETENTION = Years.ONE;

    String name;

    /**
     * Constructor.
     *
     * @param name  The name to default for other name-like values.
     */
    public DefaultLogicalTableInfo(String name) {
        this.name = name;
    }
    @Override
    public String getName() {
        return name;
    }

    /**
     * Provides a default behavior for getLongName, which is to return the unparsed table name.
     *
     * @return  A string representing the human readable name of this table.
     */
    public String getLongName() {
        return this.getName();
    }

    /**
     * Provides a default behavior for getDescription, which is to return the unparsed table name.
     *
     * @return  A string representing the description of this table's purpose.
     */
    public String getDescription() {
        return this.getName();
    }

    @Override
    public String getCategory() {
        return DEFAULT_CATEGORY;
    }

    @Override
    public Optional<ReadablePeriod> getRetention() {
        return Optional.of(DEFAULT_RETENTION);
    }
}
