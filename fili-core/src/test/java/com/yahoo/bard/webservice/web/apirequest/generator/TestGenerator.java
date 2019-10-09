// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator;

import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

/**
 * Test implementation of {@link Generator}. Simply returns the value from the constructor from the bind method. No
 * validation is performed.
 *
 * @param <T> Type of the resource that is bound.
 */
public class TestGenerator<T> implements Generator<T> {

    public T data;

    /**
     * Constructor.
     *
     * @param data the resource that will be returned by the bind method.
     */
    public TestGenerator(T data) {
        this.data = data;
    }

    @Override
    public T bind(DataApiRequestBuilder builder, RequestParameters params, BardConfigResources resources) {
        return data;
    }

    @Override
    public void validate(
            T entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        // no test validation
    }
}
