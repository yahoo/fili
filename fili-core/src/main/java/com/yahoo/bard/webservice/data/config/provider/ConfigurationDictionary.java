// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import java.util.LinkedHashMap;

/**
 * A ConfigurationDictionary is just a LinkedHashMap of String to some value type T.
 *
 * @param <T> the value type in the dictionary
 */
public class ConfigurationDictionary<T> extends LinkedHashMap<String, T> {
    private static final long serialVersionUID = 7763131056859622560L;
}
