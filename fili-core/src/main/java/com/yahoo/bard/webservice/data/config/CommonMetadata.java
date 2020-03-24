// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config;

/**
 * Metadata information that is shared between many different types of objects in Fili. To expose more specific metadata
 * information for a more constrained set of objects this interface should be extended. This is intended to be exposed
 * by classes that implement the {@link MetadataDescribable} interface.
 */
public interface CommonMetadata {

    /**
     * A unique identifier for the object.
     *
     * @return the identifier
     */
    String getId();

    /**
     * The name of the object. Does NOT have to be uniquely identifying. Defaults to id.
     *
     * @return the name
     */
    default String getName() {
        return getId();
    }

    /**
     * Description of the object. Defaults to name.
     *
     * @return the description
     */
    default String getDescription() {
        return getName();
    }

    /**
     * Utility method for providing a default implementation based on id. If you require name or description
     * configuration you must instantiate a default instance yourself.
     *
     * @param id The id for the CommonMetadata instance
     * @return the metadata
     */
    static CommonMetadata fromId(String id) {
        return new SimpleMetadata(id);
    }
}
