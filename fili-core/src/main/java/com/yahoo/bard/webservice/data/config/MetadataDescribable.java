// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config;

/**
 * An object that has metadata than can be described by the {@link SimpleMetadata} class.
 */
public interface MetadataDescribable {

    /**
     * Gets the metadata object that describes an instance of the implementing class.
     *
     * @return the metadata object
     */
    GlobalMetadata getMetadata();
}
