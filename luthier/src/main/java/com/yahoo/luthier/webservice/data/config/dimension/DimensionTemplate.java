// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.List;

/**
 * Wiki dimension config API.
 */
public interface DimensionTemplate {

    Logger LOG = LoggerFactory.getLogger(DimensionTemplate.class);

    /**
     * Get dimensions api name.
     *
     * @return dimension api name
     */
    String getApiName();

    /**
     * Get dimensions long name.
     *
     * @return dimension long name
     */
    String getLongName();

    /**
     * Get dimensions description.
     *
     * @return dimension description
     */
    String getDescription();

    /**
     * Get dimensions category.
     *
     * @return dimension category
     */
    String getCategory();

    /**
     * Get dimensions fields.
     *
     * @return a list of dimension field
     */
    LinkedHashSet<DimensionField> getFields();

    /**
     * Build a dimensionConfig instance.
     *
     * @return a dimensionConfig instance
     */
    DimensionConfig build();

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
