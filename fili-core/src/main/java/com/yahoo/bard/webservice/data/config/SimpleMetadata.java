package com.yahoo.bard.webservice.data.config;

/**
 * Default implementation of the {@link CommonMetadata} contract. For a description of the fields and their requirements
 * view the CommonMetadata javadocs.
 */
public class SimpleMetadata implements CommonMetadata {

    private String id;
    private String name;
    private String description;

    /**
     * Constructor. Id is used as name and description.
     *
     * @param id  The unique id of the object this metadata is attached to
     */
    public SimpleMetadata(String id) {
        this(id, id);
    }

    /**
     * Constructor. Name is used as description.
     *
     * @param id  The unique id of the object this metadata is attached to
     * @param name  The name of the object this metadata is attached to
     */
    public SimpleMetadata(String id, String name) {
        this(id, name, name);
    }

    /**
     * Constructor.
     *
     * @param id  The unique id of the object this metadata is attached to
     * @param name  The name of the object this metadata is attached to
     * @param description  The description of the object this metadata is attached to
     */
    public SimpleMetadata(String id, String name, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
