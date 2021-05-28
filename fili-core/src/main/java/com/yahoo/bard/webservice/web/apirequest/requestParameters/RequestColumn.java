// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.requestParameters;

import org.apache.commons.collections4.ListValuedMap;

/**
 * A class modelling request columns.
 *
 * Based on PathParameter from Jersey, typically used to describe grouping dimensions.
 */

public interface RequestColumn {

    /**
     * Get the apiName.
     * <p>
     *
     * @return the api name
     */
    String getApiName();

    /**
     * Get a map of the keys and values associated with the api name.
     *
     * @return the map of parameters
     */
    ListValuedMap<String, String> getParameters();
}
