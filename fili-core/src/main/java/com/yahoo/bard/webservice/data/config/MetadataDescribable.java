// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config;

/**
 * An object that is exposed as metadata through the metadata endpoints and describable by {@link CommonMetadata} or one
 * of its sub-interfaces.
 */
public interface MetadataDescribable {

    /**
     * Getter.
     *
     * @return the metadata object.
     */
    CommonMetadata getMetadata();
}
