// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import java.util.Locale;

/**
 * Maps a Dimension Field value to a string.
 */
@FunctionalInterface
public interface DimensionFieldNameMapper {

    /**
     * A converter function for the dimension field Mapping.
     *
     * @param dimension The dimension object used to configure the Dimension
     * @param dimensionField The dimension field object
     *
     * @return the converted string for the dimension field
     */
    String convert(Dimension dimension, DimensionField dimensionField);

    /**
     * A default implementation of the converter method which uses underscore as a separator.
     *
     * @return an instance of DimensionFieldNameMapper
     */
    static DimensionFieldNameMapper underscoreSeparatedConverter () {
        return (dimension, dimensionField) ->
                    (dimension.getApiName() + "_" + dimensionField.getName()).toUpperCase(Locale.ENGLISH);
    }
}
