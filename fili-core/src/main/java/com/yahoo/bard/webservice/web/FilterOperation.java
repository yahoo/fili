// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.util.Optional;

/**
 * Fili filter operation.
 */
public interface FilterOperation {

    /**
     * Gets the name of the operation.
     *
     * @return the name
     */
    String getName();


    /**
     * Gets the minimum number of values for this operation.
     *
     * @return the minimum number of arguments, empty if unconfigured
     */
    default Optional<Integer> getMinimumArguments() {
        return Optional.empty();
    }

    /**
     * Gets the minimum number of values for this operation.
     *
     * @return the maximum number of arguments, empty if unconfigured
     */
    default Optional<Integer> getMaximumArguments() {
        return Optional.empty();
    }

    /**
     * A string describing the argument validation rule for this operation.
     *
     * Most commonly used for creating error messages about number of arguments.
     *
     * @return A string representing the accepted number of arguments for this operation.
     */
    default String expectedRangeDescription() {

        if (getMinimumArguments().isPresent() && getMaximumArguments().isPresent()) {
            if (getMinimumArguments().get() == getMaximumArguments().get()) {
                return String.format("exactly %d arguments", getMaximumArguments().get());
            }
            return String.format(
                    "between %d and %d arguments",
                    getMinimumArguments().get(),
                    getMinimumArguments().get()
            );
        } else if (getMinimumArguments().isPresent()) {
            return String.format("as least %d arguments", getMinimumArguments().get());
        } else if (getMaximumArguments().isPresent()) {
            return String.format("no more than %d arguments", getMaximumArguments().get());
        } else {
            return "any number of arguments";
        }
    }
}
