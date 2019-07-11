// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;
import java.util.function.Supplier;

/**
 * A source for building config entities from ObjectNode configuration and a supply of factories.
 *
 * @param <T> the config concept that this is a factory source for.
 */
public class FactoryPark<T> {

    static final String ENTITY_TYPE = "factory park";

    private static final String FACTORY_KEY = "type";

    private static final String UNKNOWN_FACTORY_NAME = "factory name '%s' in config is not known to the Luthier module";

    // Use a supplier to support deferred loading
    protected final Supplier<ObjectNode> configSource;

    private final Map<String, Factory<T>> factoryMap;

    /**
     * Constructor.
     *
     * @param configSource  The source for the entity configuration.
     * @param factoryMap  The source for the entity factory to be used to build.
     */
    public FactoryPark(Supplier<ObjectNode> configSource, Map<String, Factory<T>> factoryMap) {
        this.configSource = configSource;
        this.factoryMap = factoryMap;
    }

    /**
     * Force the resolution of the underlying config and return it.
     *
     * @return  The ObjectNode describing the config for related entities.
     */
    public ObjectNode fetchConfig() {
        return configSource.get();
    }

    /**
     * Fetch the appropriate factory and construct an instance using the configSource and the factory dictionary.
     *
     * @param entityName  The name of the entity in the configSource
     * @param industrialPark  The dependency system for dependant entities.
     *
     * @return  An instance of T corresponding to this name.
     */
    T buildEntity(String entityName, LuthierIndustrialPark industrialPark) {
        LuthierValidationUtils.validateField(fetchConfig().get(entityName), ENTITY_TYPE, entityName, entityName);
        ObjectNode entityConfig = (ObjectNode) fetchConfig().get(entityName);
        LuthierValidationUtils.validateField(entityConfig.get(FACTORY_KEY), ENTITY_TYPE, entityName, FACTORY_KEY);
        String factoryName = entityConfig.get(FACTORY_KEY).textValue();
        if (! factoryMap.containsKey(factoryName)) {
            throw new LuthierFactoryException(String.format(UNKNOWN_FACTORY_NAME, factoryName));
        }
        return factoryMap.get(factoryName).build(entityName, entityConfig, industrialPark);
    }
}
