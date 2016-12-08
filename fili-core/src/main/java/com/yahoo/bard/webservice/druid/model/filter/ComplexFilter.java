// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import java.util.List;

/**
 * A Druid filter that is defined by applying an operation to at least one other filter. For example, {@code not} and
 * {@code and} filters are complex. A {@code selector} filter is not.
 */
public interface ComplexFilter {

    /**
     * Returns the filters that are operated on by this filter.
     *
     * @return The filters operated on by this filter
     */
    List<Filter> getFields();
}
