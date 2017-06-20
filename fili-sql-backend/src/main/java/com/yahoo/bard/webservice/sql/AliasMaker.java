// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import java.util.function.Function;

/**
 * Creates aliases by prepending to a string.
 */
public class AliasMaker implements Function<String, String> {
    private final String preprend;

    /**
     * Construct the alias maker with a given string.
     *
     * @param preprend  The string to prepend when making an alias.
     */
    public AliasMaker(String preprend) {
        this.preprend = preprend;
    }

    @Override
    public String apply(String input) {
        return preprend + input;
    }

    /**
     * If a string starts with the prepended string, remove it.
     *
     * @param input  The string to attempt to unalias.
     *
     * @return the unaliased string.
     */
    public String unApply(String input) {
        if (input.startsWith(preprend)) {
            return input.replace(preprend, "");
        } else {
            return input;
        }
    }
}
