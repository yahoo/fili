// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

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
    lte(1, "notgt", "notGreater", "notGreaterThan"),
    gt(1, "greater"),
    gte(1, "notlt", "notLess", "notLessThan"),
    // The lower bound is inclusive and the upper bound is exclusive. (so it is gte and lt operators together)
    between(2, "bet")
    ;

    private static final Map<String, DefaultFilterOperation> ALIASES = new HashMap<>();

    private final List<String> aliases;
    private final Integer minimumArguments;
    private final Integer maximumArguments;

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
        this((Integer) null, (Integer) null);
    }

    /**
     * Constructor.
     *
     * Default to no minimum or maximum number of arguments.
     *
     * @param aliases  List of aliases for the op.
     */
    DefaultFilterOperation(String... aliases) {
        this(null, null, aliases);
    }

    /**
     * Constructor.
     *
     * @param fixedArgumentSize Require exactly this many arguments
     * @param aliases  List of aliases for the op.
     */
    DefaultFilterOperation(Integer fixedArgumentSize, String... aliases) {
        this(fixedArgumentSize, fixedArgumentSize, aliases);
    }

    /**
     * Constructor.
     *
     * @param minimumArguments  The minimum number of allowed arguments for this operation
     * @param maximumArguments  The maxmimum number of allowed arguments for this operation
     * @param aliases  List of aliases for the op.
     */
    DefaultFilterOperation(Integer minimumArguments, Integer maximumArguments, String... aliases) {
        this.aliases = Arrays.asList(aliases);
        this.minimumArguments = minimumArguments;
        this.maximumArguments = maximumArguments;
    }

    @Override
    public String getName() {
        return name();
    }

    /**
     * Convert from a string to a filter operation.
     *
     * @param value  Candidate string
     *
     * @return  the filter operation if one was found
     * @throws IllegalArgumentException if no filter operation was found
     */
    static public DefaultFilterOperation fromString(@NotNull String value) throws IllegalArgumentException {
        return Optional.ofNullable(ALIASES.get(value))
                .orElseThrow(() -> new IllegalArgumentException("unknown filter operation: " + value));
    }

    @Override
    public Optional<Integer> getMinimumArguments() {
        return Optional.ofNullable(minimumArguments);
    }

    @Override
    public Optional<Integer> getMaximumArguments() {
        return Optional.ofNullable(maximumArguments);
    }
}
