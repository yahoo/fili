// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import com.yahoo.bard.webservice.util.EnumUtils;

/**
 * Bard test dimensions.
 */
public enum TestDimensionName implements DimensionName {
    NAME,
    AGE,
    ETHNICITY,
    GENDER
    ;

    @Override
    public String asName() {
      return EnumUtils.camelCase(name());
    }
}
