package com.yahoo.bard.webservice.data.config;

/**
 * An object that is exposed as metadata through the metadata endpoints and describable by {@link GlobalMetadata} or one
 * of its sub-interfaces.
 */
public interface MetadataDescribable {

    GlobalMetadata getMetadata();
}
