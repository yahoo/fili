// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Interface for types of DataSources.
 * <p>
 * See {@link DefaultDataSourceType} for all types of Druid data sources.
 */
@FunctionalInterface
public interface DataSourceType {

    /**
     * Returns the JSON serialization of this data source type.
     *
     * @return the JSON representation of this type
     */
    @JsonValue
    String toJson();
}
