package com.yahoo.wiki.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.wiki.webservice.data.config.Template;

import java.util.HashMap;
import java.util.LinkedHashSet;

public class WikiDimensionConfig extends Template {

    @JsonProperty("fieldSets")
    private HashMap<String, LinkedHashSet<WikiDimensionFieldTemplate>> fieldSets;

    @JsonProperty("dimensions")
    private LinkedHashSet<WikiDimensionTemplate> dimensions;

    /**
     * Constructor.
     */
    public WikiDimensionConfig() {
    }

    /**
     * Set dimensions configuration info.
     */
    public void setFields(HashMap<String, LinkedHashSet<WikiDimensionFieldTemplate>> fieldSets) {
        this.fieldSets = fieldSets;
    }

    public void setDimensions(LinkedHashSet<WikiDimensionTemplate> dimensions) {
        this.dimensions = dimensions;
    }

    /**
     * Get dimensions configuration info.
     */
    public HashMap<String, LinkedHashSet<WikiDimensionFieldTemplate>> getFieldSets() {
        return this.fieldSets;
    }

    public LinkedHashSet<WikiDimensionTemplate> getDimensions() {
        return this.dimensions;
    }


}
