// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import static com.yahoo.bard.webservice.druid.model.having.Having.DefaultHavingType;

import com.yahoo.bard.webservice.druid.model.having.HavingType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.validation.constraints.NotNull;

/**
 * Types of legal having operations
 */
public enum HavingOperation {
    equalTo(DefaultHavingType.EQUAL_TO, false, "equals", "eq"),
    greaterThan(DefaultHavingType.GREATER_THAN, false, "greater", "gt"),
    lessThan(DefaultHavingType.LESS_THAN, false, "less", "lt"),
    notEqualTo(DefaultHavingType.EQUAL_TO, true, "notEquals", "noteq", "neq"),
    notGreaterThan(DefaultHavingType.GREATER_THAN, true, "notGreater", "notgt", "lte"),
    notLessThan(DefaultHavingType.LESS_THAN, true, "notLess", "notlt", "gte");

    private static final Map<String, HavingOperation> ALIASES = new HashMap<>();

    private final List<String> aliases;
    private final HavingType type;
    private final boolean negated;

    static {
        for (HavingOperation op : HavingOperation.values()) {
            ALIASES.put(op.name(), op);
            for (String alias : op.aliases) {
                ALIASES.put(alias, op);
            }
        }
    }

    static public HavingOperation fromString(@NotNull String value) {
        return Optional.ofNullable(ALIASES.get(value))
                .orElseThrow(() -> new IllegalArgumentException("unknown having operation: " + value));
    }

    HavingOperation(HavingType type, boolean negated, String... aliases) {
        this.type = type;
        this.aliases = Arrays.asList(aliases);
        this.negated = negated;
    }

    public boolean isNegated() {
        return negated;
    }

    public HavingType getType() {
        return type;
    }
}
