// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

/**
 * Parse External Config from json file.
 */
public class ExternalConfigLoader {

    /**
     * Parse the external file into corresponding templates.
     *
     * @param externalConfigFilePath The external file's url containing the external config information
     * @param template The external config template type
     * @param objectMapper ObjectMapper instance
     * @param <T> The external config template
     * @return Template instance parsed from the external file
     */
    public <T> T parseExternalFile(String externalConfigFilePath, Class<T> template, ObjectMapper objectMapper) {
        try {
            File configFile = new File(externalConfigFilePath);
            JsonNode configurator = objectMapper.readTree(configFile);
            return objectMapper.convertValue(configurator, template);
        } catch (IOException exception) {
            String message = "Could not find external config file located at " +
                    "url: " + externalConfigFilePath;
            throw new IllegalStateException(message, exception);
        }
    }
}
