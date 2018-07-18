// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.dimension;

import java.util.LinkedHashSet;

/**
 * Dimension Field List Template.
 */
public interface DimensionFieldListTemplate {

    /**
     * Set fieldset's name.
     *
     * @param name fieldset's name
     */
    void setFieldName(String name);

    /**
     * Set field list.
     *
     * @param list a list of field
     */
    void setFieldList(LinkedHashSet<DimensionFieldInfoTemplate> list);

    /**
     * Get fieldset's name.
     *
     * @return fieldset's name
     */
    String getFieldName();

    /**
     * Get field list.
     *
     * @return a list of field
     */
    LinkedHashSet<DimensionFieldInfoTemplate> getFieldList();
}
