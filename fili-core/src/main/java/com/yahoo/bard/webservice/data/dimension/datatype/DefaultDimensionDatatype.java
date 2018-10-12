// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.datatype;

import java.util.Locale;

/**
 * Default set of dimension datatypes.
 */
public enum DefaultDimensionDatatype implements DimensionDatatype {
    TEXT,
    NUMBER,
    DATE
    ;

    @Override
    public String getName() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public boolean isA(DimensionDatatype otherType) {
        return otherType == this;
    }
}
