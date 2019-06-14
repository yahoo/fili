package com.yahoo.bard.webservice.config.luthier.factories;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.config.LuthierDimensionField;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.LinkedHashSet;
import java.util.ArrayList;
import java.util.List;


public class KeyValueStoreDimensionFactory implements Factory<Dimension> {


    /**
     * @param fieldsNode the JsonNode object that points to the content of "fields" key
     *                   or "defaultFields" key
     * @return constructed fields associated with a dimension. Contains camelName, description, and tags
     */
    private LinkedHashSet<DimensionField> fieldsBuilder(JsonNode fieldsNode) {
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>();
        for(JsonNode node : fieldsNode) {
            List<String> tags = new ArrayList<>();
            if (node.has("tags")) {
                for (final JsonNode strNode : node.get("tags")) {
                    tags.add( strNode.textValue() );
                }
            }
            dimensionFields.add(new LuthierDimensionField(
                    EnumUtils.camelCase(node.get("name").textValue()),
                    "Error: currently there is no description",             // TODO: Magic values!
                    tags
            ));
        }
        return dimensionFields;
    }

    /**
     * Build a dimension instance.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  the json tree describing this config entity
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return  A newly constructed config instance for the name and config provided
     */
    @Override
    public Dimension build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories) {
        String dimensionName = name;
        String longName = configTable.get("longName").textValue();
        String category = configTable.get("category").textValue();
        String description = configTable.get("description").textValue();
        LinkedHashSet<DimensionField> dimensionFields = fieldsBuilder(
                configTable.get("fields")
        );
        KeyValueStore keyValueStore = resourceFactories.getKeyValueStore(
                configTable.get("description").textValue()
        );
        SearchProvider searchProvider = resourceFactories.getSearchProvider(
                configTable.get("searchProvider").textValue()
        );
        LinkedHashSet<DimensionField> defaultDimensionFields= fieldsBuilder(
                configTable.get("defaultFields")
        );
        boolean isAggregatable = configTable.get("isAggregatable").booleanValue();

        Dimension dimension = new KeyValueStoreDimension(
                dimensionName,
                longName,
                category,
                description,
                dimensionFields,
                keyValueStore,
                searchProvider,
                defaultDimensionFields,
                isAggregatable
        );

        return dimension;
    }
}
