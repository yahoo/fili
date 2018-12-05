// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.validation.constraints.NotNull;

/**
 * Types of legal filter operations.
 */
public enum DefaultFilterOperation implements FilterOperation {
    in,
    notin,
    startswith,
    contains,
    eq("equals"),
    lt("less"),
    lte("notgt", "notGreater", "notGreaterThan"),
    gt("greater"),
    gte("notlt", "notLess", "notLessThan"),
    // The lower bound is inclusive and the upper bound is exclusive. (so it is gte and lt operators together)
    between("bet")
    ;

    private static final Map<String, DefaultFilterOperation> ALIASES = new HashMap<>();

    private final List<String> aliases;

    static {
        for (DefaultFilterOperation op : DefaultFilterOperation.values()) {
            ALIASES.put(op.name(), op);
            for (String alias : op.aliases) {
                ALIASES.put(alias, op);
            }
        }
    }

    /**
     * Constructor.
     */
    DefaultFilterOperation() {
        this.aliases = new ArrayList<>();
    }

    /**
     * Constructor.
     *
     * @param aliases  List of legal aliases for the op.
     */
    DefaultFilterOperation(String... aliases) {
        this.aliases = Arrays.asList(aliases);
    }

    @Override
    public String getName() {
        return name();
    }

    /**
     * Convert from a string to a Having operation.
     *
     * @param value  Candidate string
     *
     * @return  the Having if one was found
     * @throws IllegalArgumentException if no Having was found
     */
    static public DefaultFilterOperation fromString(@NotNull String value) throws IllegalArgumentException {
        return Optional.ofNullable(ALIASES.get(value))
                .orElseThrow(() -> new IllegalArgumentException("unknown filter operation: " + value));
    }
}
