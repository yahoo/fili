// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * A supplier for config with the core assumption that the config will be at a resource location on the classpath.
 */
public class ResourceNodeSupplier implements Supplier<ObjectNode> {

    public static final String NOT_AN_OBJECT = "%s is not formatted as a JSON Object";
    public static final String LOAD_FAILURE = "Can't load resource: %s";
    private final String resourceName;

    ObjectNode objectNode;

    /**
     * Constructor.
     *
     * @param resourceName  resource name for config file
     */
    public ResourceNodeSupplier(String resourceName) {
        this.resourceName = resourceName;
    }

    @Override
    public ObjectNode get() {
        if (resourceName != null) {
            try {
                JsonNode node = new ObjectMapper().reader()
                        .readTree(this.getClass().getClassLoader().getResourceAsStream(resourceName));
                if (!(node instanceof ObjectNode)) {
                    String message = String.format(NOT_AN_OBJECT, resourceName);
                    throw new LuthierFactoryException(message);
                }
                objectNode = (ObjectNode) node;
            } catch (IOException e) {
                String message = String.format(LOAD_FAILURE, resourceName);
                throw new LuthierFactoryException(message, e);
            }
        }
        return objectNode;
    }
}
