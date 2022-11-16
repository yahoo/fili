// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache;

import java.io.Serializable;

/**
 * Dummy Cache implementation.
 *
 * @param <T> The type of the cache values
 */
public class StubDataCache<T extends Serializable> implements DataCache<T> {

    @Override
    public T get(String key) {
        return null;
    }

    @Override
    //(Deprecate this return type to be void)
    public boolean set(String key, T value) {
        return Boolean.FALSE;
    }

    @Override
    public void clear() {
        // do nothing
    }
}
