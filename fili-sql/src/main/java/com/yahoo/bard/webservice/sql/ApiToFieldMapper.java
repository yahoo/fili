// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import com.yahoo.bard.webservice.table.PhysicalTableSchema;

import java.util.function.Function;

/**
 * Maps between logical and physical column names given a table schema.
 */
public class ApiToFieldMapper implements Function<String, String> {
    private final PhysicalTableSchema physicalTableSchema;

    /**
     * Construct the alias maker with the given schema.
     *
     * @param physicalTableSchema  The physical table schema which maps between logical and physical column names.
     */
    public ApiToFieldMapper(PhysicalTableSchema physicalTableSchema) {
        this.physicalTableSchema = physicalTableSchema;
    }

    @Override
    public String apply(String input) {
        return physicalTableSchema.getPhysicalColumnName(input);
    }

    /**
     * Finds the api name given a field name and if none are found then it returns the given input.
     *
     * @param input  The string to attempt to unalias.
     *
     * @return the api name of the column or the original input if no mapping was found.
     */
    public String unApply(String input) {
        return physicalTableSchema.getLogicalColumnNames(input).stream().findFirst().orElse(input);
    }
}
