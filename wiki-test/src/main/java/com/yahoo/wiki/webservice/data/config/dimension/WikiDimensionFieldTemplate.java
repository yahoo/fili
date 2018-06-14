package com.yahoo.wiki.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.LinkedList;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiDimensionFieldTemplate implements DimensionField {

    @JsonProperty("name")
    private String name;

    @JsonProperty("description")
    private String description;

    @JsonProperty("tags")
    private LinkedList<String> tags;

    /**
     * Constructor.
     */
    public WikiDimensionFieldTemplate() { }

    /**
     * Set dimensions info.
     */
    public void setName(String name) {
        this.name = name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setTags(LinkedList<String> tags) {
        this.tags = new LinkedList<>(tags);
    }

    /**
     * Get dimensions info.
     */
    @Override
    public String getName() { return EnumUtils.camelCase(this.name); }

    @Override
    public String getDescription() {
        return this.description;
    }

    public LinkedList<String> getTags() { return this.tags; }

    @Override
    public String toString() {
        return EnumUtils.camelCase(this.getName());
    }

}
