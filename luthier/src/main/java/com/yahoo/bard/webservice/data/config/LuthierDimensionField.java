package com.yahoo.bard.webservice.data.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.ArrayList;

public class LuthierDimensionField implements DimensionField {

    private String camelName;
    private String description;
    private ArrayList<String> tags;

    private LuthierDimensionField(String camelName, String description, JsonNode node) {
        this.description = camelName;
        this.description = description;
        ArrayList<String> tags = new ArrayList<>();
        if (node.get("tags").isArray()) {
            for (final JsonNode strNode : node.get("tags")) {
                tags.add(strNode.textValue());
            }
        }
        this.tags = tags;
    }

    public LuthierDimensionField(JsonNode node) {
        this(   EnumUtils.camelCase( node.get("name").textValue() ),
                "Error: currently there is no description",
                node );

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
