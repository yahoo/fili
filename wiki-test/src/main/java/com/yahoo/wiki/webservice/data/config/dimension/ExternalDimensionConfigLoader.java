package com.yahoo.wiki.webservice.data.config.dimension;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.MalformedURLException;
import java.net.URL;
import java.io.IOException;
import java.util.*;
import java.io.File;

/**
 * Loads a single dimension config json file and builds DimensionConfigs into a dimension dictionary.
 */
public class ExternalDimensionConfigLoader {

    private final ObjectMapper objectMapper;
    private LinkedHashSet<WikiDimensionTemplate> dimensions;
    private HashMap<String, LinkedHashSet<WikiDimensionFieldTemplate>> fields;

    /**
     * Constructor.
     *
     * @param objectMapper a mapper to deserialize configurations
     */
    public ExternalDimensionConfigLoader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Parse the external file into corresponding templates.
     *
     * @param externalConfigFileURL  The external file's url containing the dimension config information
     *
     * @return Templates parsed from the external file
     */
    public WikiDimensionConfig loadDimensionConfigs(File externalConfigFile) {
        try {
            JsonNode dimensionConfigurator = objectMapper.readTree(externalConfigFile);
            return objectMapper.convertValue(dimensionConfigurator, WikiDimensionConfig.class);
        } catch (IOException exception) {
            String message = "Could not parse due to invalid schema in external dimension config file located at " +
                    "url: " + externalConfigFile.getPath();
            throw new RuntimeException(message, exception);
        }
    }

    /**
     * Get the URL to the external config file which is used to load the templates from.
     *
     * @return a string of the url to the external config file
     */
    public File getExternalConfigFile() {

        File targetFile = new File("DimensionConfigTemplateSample.json");

        return targetFile;

    }

}

