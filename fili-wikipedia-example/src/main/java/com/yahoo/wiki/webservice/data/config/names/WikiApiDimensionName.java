// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.names;

import com.yahoo.bard.webservice.util.EnumUtils;

/**
 * Hold all the Wikipedia API dimension names.
 */
public enum WikiApiDimensionName {
    PAGE,
    LANGUAGE,
    USER,
    UNPATROLLED,
    NEW_PAGE,
    ROBOT,
    ANONYMOUS,
    NAMESPACE,
    CONTINENT,
    COUNTRY,
    REGION,
    CITY;

    private final String camelName;

    /**
     * Constructor.
     */
    WikiApiDimensionName() {
        this.camelName = EnumUtils.camelCase(name());
    }

    /**
     * View the dimension name.
     *
     * @return The camelCase version of the dimension.
     */
    public String asName() {
        return camelName;
    }
}
