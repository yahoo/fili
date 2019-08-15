// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.names;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.util.EnumUtils;

/**
 * Used to instantiate a TableName object for Luthier Configuration.
 */
public class LuthierTableName implements TableName {
    private final String camelName;
    private final static char UNDERSCORE = '_';

    /**
     * Constructor.
     *
     * @param name the name of this Luthier Table
     */
    public LuthierTableName(String name) {
        if (isAllCaps(name)) {
            // if name argument is in ALL_CAPS_CASE, convert to camelCase
            camelName = EnumUtils.camelCase(name);
        } else {
            // otherwise, assume that the user has sanitized this into camelCaseName already.
            camelName = name;
        }
    }

    @Override
    public String asName() {
        return camelName;
    }

    /**
     * Helper Util to determine an ALL_CAPS_CASE word.
     *
     * @param word  a word we want to test for
     * @return true if the word is ALL_CAPS_CASE (i.e. made of A-Z or _ ) false otherwise
     */
    private Boolean isAllCaps(String word) {
        for (int i = 1; i < word.length(); i++) {
            char character = word.charAt(i);
            if (Character.isLetter(character) && Character.isLowerCase(character)) {
                return false;
            } else if (character != UNDERSCORE) {
                return false;
            }
        }
        return true;
    }
}
