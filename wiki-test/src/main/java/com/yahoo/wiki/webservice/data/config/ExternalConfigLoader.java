package com.yahoo.wiki.webservice.data.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.wiki.webservice.data.config.Template;

import java.io.IOException;
import java.util.*;
import java.io.File;

/**
 * Loads a single dimension config json file and builds DimensionConfigs into a dimension dictionary.
 */
public class ExternalConfigLoader {

    private final ObjectMapper objectMapper;

    /**
     * Constructor.
     *
     * @param objectMapper a mapper to deserialize configurations
     */
    public ExternalConfigLoader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Parse the external file into corresponding templates.
     *
     * @param externalConfigFilePath The external file's url containing the dimension config information
     * @return Templates parsed from the external file
     */
    public Template parseExternalFile(String externalConfigFilePath, Class<?> template) {
        try {
            File ConfigFile = new File(externalConfigFilePath);
            JsonNode configurator = objectMapper.readTree(ConfigFile);
            return (Template) objectMapper.convertValue(configurator, template);
        } catch (IOException exception) {
            String message = "Could not parse due to invalid schema in external config file located at " +
                    "url: " + externalConfigFilePath;
            throw new RuntimeException(message, exception);
        }
    }

}

