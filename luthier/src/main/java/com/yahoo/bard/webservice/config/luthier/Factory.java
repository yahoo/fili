// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A factory method to create a config entity using Json config and the LuthierIndustrialPark to resolve dependencies.
 *
 * @param <T> the config concept that this is a factory for.
 */
public interface Factory<T> {

    /**
     * Build a dimension instance.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  the json tree describing this config entity
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return  A newly constructed config instance instance for the name and config provided
     */
    T build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories);
}
