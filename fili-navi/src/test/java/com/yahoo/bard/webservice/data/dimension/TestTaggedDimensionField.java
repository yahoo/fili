// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.Collections;
import java.util.Set;

/**
 * Test implementation of TaggedDimensionField used in testings.
 */
public enum TestTaggedDimensionField implements TaggedDimensionField {
    TEST_NO_TAG,
    TEST_ONE_TAG,
    TEST_TWO_TAG
    ;

    private final String name;
    private Set<? extends Tag> tags;

    /**
     * Constructor.
     */
    TestTaggedDimensionField() {
        this.name = EnumUtils.camelCase(name());
        this.tags = Collections.emptySet();
    }

    public void setTags(Set<? extends Tag> tags) {
        this.tags = tags;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return getName() + " description";
    }

    @Override
    public Set<? extends Tag> getTags() {
        return tags;
    }
}
