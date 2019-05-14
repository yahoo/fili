// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A wrapper class to simplify column name aliasing in response formatting.
 *
 * This is particularly useful in supporting both aliased columns where there's an underlying (different) schema that
 * needs to be matched as well as default columns, for simpler use cases where there is no renaming.
 */
public class NameAliasList extends java.util.ArrayList<Map.Entry<String, String>> {

    /**
     * Constructor.
     *
     * @param map Map copy constructor.
     */
    protected NameAliasList(List<Map.Entry<String, String>> map) {
        super(map);
    }

    /**
     * Static equivalent to the base constructor.
     *
     * @param entries  A list of entries to be constructed from.
     *
     * @return A nameAliaslist
     */
    public static NameAliasList fromEntries(List<Map.Entry<String, String>> entries) {
        return new NameAliasList(entries);
    }

    /**
     * Static convenience method to turn lists of strings into lists of aliases.
     *
     * @param names  A list of column names mapped to themselves.
     *
     * @return  A nameAliasList with all keys equal to their values.
     */
    public static NameAliasList fromNames(List<String> names) {
        return new NameAliasList(
                names.stream()
                        .map(it -> new AbstractMap.SimpleEntry<String, String>(it, it))
                        .collect(Collectors.toList()));
    }
};
