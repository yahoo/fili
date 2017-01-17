// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import static com.yahoo.bard.webservice.data.dimension.impl.DefaultDimensionFieldTag.PRIMARY_KEY;

import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test implementation of TaggedDimensionField used in testings.
 */
public enum TestTaggedDimensionField implements TaggedDimensionField {
    TEST_PRIMARY_KEY(Collections.singletonList(PRIMARY_KEY)),
    TEST_DISPLAY_NAME(Arrays.asList(PRIMARY_KEY, PRIMARY_KEY)),
    TEST_DESCRIPTION(Collections.<Tag>emptyList())
    ;

    private final String name;
    private final List<? extends Tag> tags;

    /**
     * Constructor.
     *
     * @param tags tags associated with the current dimension field
     */
    TestTaggedDimensionField(List<? extends Tag> tags) {
        this.name = EnumUtils.camelCase(name());
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
    public List<? extends Tag> getTags() {
        return tags;
    }
}
