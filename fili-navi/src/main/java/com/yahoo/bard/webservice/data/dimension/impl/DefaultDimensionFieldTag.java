// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.dimension.Tag;
import com.yahoo.bard.webservice.util.EnumUtils;

/**
 * Default dimension field tag provided to match fili core concepts.
 */
public enum DefaultDimensionFieldTag implements Tag {
    /**
     * Dimension field tag to match fili concept of a "key" field.
     */
    PRIMARY_KEY
    ;

    private final String tagName;

    /**
     * Constructor, build name using camel cased enum name.
     */
    DefaultDimensionFieldTag() {
        this.tagName = EnumUtils.camelCase(name());
    }

    @Override
    public String getName() {
        return tagName;
    }
}
