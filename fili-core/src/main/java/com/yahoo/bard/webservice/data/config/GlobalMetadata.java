package com.yahoo.bard.webservice.data.config;

/**
 * Metadata information that is shared between all objects that can be exposed as metadata
 */
public interface GlobalMetadata {

    String getId();

    default String getName() {
        return getId();
    }

    default String getDescription() {
        return getName();
    }

    static GlobalMetadata fromId(String id) {
        return new SimpleMetadata(id);
    }
}
