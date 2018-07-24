// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.dimension;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Dimension Field List Template.
 */
public interface DimensionFieldListTemplate {

    Logger LOG = LoggerFactory.getLogger(DimensionFieldListTemplate.class);

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
    void setFieldList(List<DimensionFieldInfoTemplate> list);

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
    List<DimensionFieldInfoTemplate> getFieldList();

    /**
     * Sort fieldList to make sure 'id' field to be the first.
     *
     * @param fieldList unsorted fieldList
     */
    default void sortFields(List<DimensionFieldInfoTemplate> fieldList) {
        fieldList.sort((o1, o2) -> {
            if ("id".equals(o1.getFieldName()) || "ID".equals(o1.getFieldName())) {
                return -1;
            }
            if ("id".equals(o2.getFieldName()) || "ID".equals(o2.getFieldName())) {
                return 1;
            }
            return 0;
        });
        if (!"ID".equals(fieldList.get(0).getFieldName())
                && !"id".equals(fieldList.get(0).getFieldName())) {
            LOG.error("No 'ID' field found in dimension field.");
            throw new RuntimeException("No 'ID' field found in dimension field..");
        }

    }
}
