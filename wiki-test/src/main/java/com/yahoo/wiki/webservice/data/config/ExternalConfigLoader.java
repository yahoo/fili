// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.File;

/**
 * Parse External Config from json file.
 */
public class ExternalConfigLoader {

    private final ObjectMapper objectMapper;

    /**
     * Constructor.
     *
     * @param objectMapper a mapper to deserialize external configurations
     */
    public ExternalConfigLoader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Parse the external file into corresponding templates.
     *
     * @param externalConfigFilePath The external file's url containing the external config information
     * @param template               The external config template type
     * @return Template instance parsed from the external file
     */
    public Template parseExternalFile(String externalConfigFilePath, Class<?> template) {
        try {
            File configFile = new File(externalConfigFilePath);
            JsonNode configurator = objectMapper.readTree(configFile);
            return (Template) objectMapper.convertValue(configurator, template);
        } catch (IOException exception) {
            String message = "Could not parse due to invalid schema in external config file located at " +
                    "url: " + externalConfigFilePath;
            throw new IllegalStateException(message, exception);
        }
    }
}
