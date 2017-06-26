// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import com.yahoo.bard.webservice.table.PhysicalTableSchema;

import java.util.function.Function;

/**
 * Creates aliases by prepending to a string.
 */
public class AliasMaker implements Function<String, String> {
    private final PhysicalTableSchema physicalTableSchema;

    /**
     * Construct the alias maker with a given string.
     *
     * @param physicalTableSchema
     */
    public AliasMaker(PhysicalTableSchema physicalTableSchema) {
        this.physicalTableSchema = physicalTableSchema;
    }

    @Override
    public String apply(String input) {
        return physicalTableSchema.getPhysicalColumnName(input);
    }

    /**
     * If a string starts with the prepended string, remove it.
     *
     * @param input  The string to attempt to unalias.
     *
     * @return the unaliased string.
     */
    public String unApply(String input) {
        return physicalTableSchema.getLogicalColumnNames(input).stream().findFirst().orElse(input);
    }
}
