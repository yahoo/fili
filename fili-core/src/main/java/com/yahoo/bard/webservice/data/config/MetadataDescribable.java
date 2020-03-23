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
