package com.yahoo.bard.webservice.data.config;

public class SimpleMetadata implements GlobalMetadata {

    private String id;
    private String name;
    private String description;

    public SimpleMetadata(String id) {
        this(id, id);
    }

    public SimpleMetadata(String id, String name) {
        this(id, name, name);
    }

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
