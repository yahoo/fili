package com.yahoo.bard.webservice.config.luthier.factories;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.config.LuthierDimensionField;
import com.yahoo.bard.webservice.data.config.dimension.DefaultKeyValueStoreDimensionConfig;
import com.yahoo.bard.webservice.data.dimension.*;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProvider;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;
import com.yahoo.bard.webservice.util.Utils;
import org.joda.time.DateTime;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class KeyValueStoreDimensionFactory implements Factory<Dimension> {

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
        String dimensionName = configTable.get("apiName").textValue();
        assert( name == dimensionName );                                // redundancy in the JSON config file
        String longName = configTable.get("longName").textValue();
        String category = "UNKNOWN_CATEGORY";
        String description = configTable.get("description").textValue();
        KeyValueStore keyValueStore = resourceFactories.getKeyValueStore(configTable.get("description").textValue());
        SearchProvider searchProvider = resourceFactories.getSearchProvider(configTable
                                                                            .get("searchProvider").textValue() );
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>();
        for(JsonNode node : configTable.get("fields")) {
            dimensionFields.add( new LuthierDimensionField((node)) );
        }
        boolean isAggregatable = true;
        LinkedHashSet<DimensionField> defaultDimensionFields = dimensionFields;

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
