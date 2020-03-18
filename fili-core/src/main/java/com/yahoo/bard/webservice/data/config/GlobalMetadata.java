package com.yahoo.bard.webservice.data.config;

/**
 * Data object that is exposes metadata fields common to all objects exposed on metadata endpoints.
 * {@link SimpleMetadata} is the default and simplest implementation of this contract.
 */
public interface GlobalMetadata {

    /**
     * // TODO fix description
     * Unique identifier over (all objects or all objects of the same type?). Ensuring uniqueness is the responsibility
     * of the client system.
     *
     * @return the identifier
     */
    String getId();

    /**
     * Logical name of the attached object. Does not need to be uniquely identifying.
     *
     * @return the name
     */
    default String getName() {
        return getId();
    }

    /**
     * Description of the attached object.
     *
     * @return the description
     */
    default String getDescription() {
        return getName();
    }

    /**
     * Utility method to produce a basic implementation of this from an id.
     *
     * @param id  The unique id to generate the metadata instance with.
     * @return the metadata implementation
     */
    static GlobalMetadata fromId(String id) {
        return new SimpleMetadata(id);
    }
}
