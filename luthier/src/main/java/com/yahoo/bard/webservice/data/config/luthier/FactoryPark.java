// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode;
import com.yahoo.bard.webservice.exceptions.LuthierFactoryException;

import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * A source for building config entities from LuthierConfigNode configuration and a supply of factories.
 *
 * @param <T> the config concept that this is a factory source for.
 */
public class FactoryPark<T> {

    private static final String FACTORY_KEY = "type";

    private static final String UNKNOWN_FACTORY_NAME = "when configuring entity '%s' in '%s', " +
            "factory name '%s' in config is not known to the Luthier module";

    // Use a supplier to support deferred loading
    protected final LuthierSupplier configSource;

    private final Map<String, Factory<T>> factoryMap;

    /**
     * Constructor.
     *
     * @param configSource  The source for the entity configuration.
     * @param factoryMap  The source for the entity factory to be used to build.
     */
    public FactoryPark(LuthierSupplier configSource, @NotNull Map<String, Factory<T>> factoryMap) {
        this.configSource = configSource;
        assert factoryMap != null;
        this.factoryMap = factoryMap;
    }

    /**
     * Force the resolution of the underlying config and return it.
     *
     * @return  The LuthierConfigNode describing the config for related entities.
     */
    public LuthierConfigNode fetchConfig() {
        return configSource.get();
    }

    /**
     * Fetch the appropriate factory and construct an instance using the configSource and the factory dictionary.
     *
     * @param entityName  The name of the entity in the configSource
     * @param industrialPark  The dependency system for dependant entities.
     * @param conceptType  The concept we're building an entity for
     *
     * @return  An instance of T corresponding to this name.
     */
    T buildEntity(String entityName, LuthierIndustrialPark industrialPark, ConceptType conceptType) {
        LuthierConfigNode configuration = fetchConfig();
        LuthierConfigNode entityConfig = configuration.get(entityName);
        if (entityConfig == null) {
            throw new LuthierFactoryException(
                String.format("Unknown %s: '%s'", conceptType.getConceptKey(), entityName)
            );
        }

        LuthierConfigNode type = entityConfig.get(FACTORY_KEY);
        if (type == null) {
            throw new LuthierFactoryException(String.format(
                    "No %s type provided for '%s'. Don't know what to build.",
                    conceptType.getConceptKey(),
                    entityName
            ));
        }
        String factoryName = entityConfig.get(FACTORY_KEY).textValue();

        if (!factoryMap.containsKey(factoryName)) {
            throw new LuthierFactoryException(
                    String.format(
                            UNKNOWN_FACTORY_NAME,
                            entityName,
                            configSource.getResourceName(),
                            factoryName
                    )
            );
        }
        return factoryMap.get(factoryName).build(entityName, entityConfig, industrialPark);
    }
}
