package com.yahoo.bard.webservice.data.config;

import com.yahoo.bard.webservice.data.dimension.DimensionField;

import java.util.List;

public class LuthierDimensionField implements DimensionField {

    private String camelName;
    private String description;
    private List<String> tags;

    public LuthierDimensionField(String camelName, String description, List<String> tags) {
        this.camelName = camelName;
        this.description = description;
        this.tags = tags;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getName() {
        return camelName;
    }

    @Override
    public String toString() {
        return getName();
    }
}
