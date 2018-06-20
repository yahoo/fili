// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.DimensionName;

/**
 * Wiki dimension config API.
 */
public interface DimensionConfigAPI extends DimensionName {

    /**
     * Set dimensions api name.
     *
     * @param apiName dimension api name
     */
    void setApiName(String apiName);

    /**
     * Set dimensions long name.
     *
     * @param longName dimension long name
     */
    void setLongName(String longName);

    /**
     * Set dimensions description.
     *
     * @param description dimension description
     */
    void setDescription(String description);

    /**
     * Set dimensions category.
     *
     * @param category dimension category
     */
    void setCategory(String category);

    /**
     * Set dimensions fields.
     *
     * @param fields dimension fields
     */
    void setFields(WikiDimensionFieldConfigTemplate fields);

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
     * @return dimension field
     */
    WikiDimensionFieldConfigTemplate getFields();

}
