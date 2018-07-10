// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.DimensionName;
import com.yahoo.bard.webservice.data.dimension.DimensionField;

import java.util.HashMap;
import java.util.LinkedHashSet;

/**
 * Wiki dimension config API.
 */
public interface DimensionConfigAPI extends DimensionName {

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
     * @param fieldDictionary a map from fieldset's name to fieldset
     * @return a list of dimension field
     */
    LinkedHashSet<DimensionField> getFields(HashMap<String,
            LinkedHashSet<DimensionFieldSetsTemplate>> fieldDictionary);

}
