package com.yahoo.bard.webservice.data;

import com.yahoo.bard.webservice.data.config.SimpleMetadata;

/**
 * An object that has metadata than can be described by the {@link SimpleMetadata} class.
 */
public interface MetadataDescribable {

    /**
     * Gets the metadata object that describes an instance of the implementing class.
     */
    SimpleMetadata getMetadata();
}
