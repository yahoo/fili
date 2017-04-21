// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

import static com.yahoo.bard.webservice.config.ConfigMessageFormat.INVALID_MODULE_CONFIGURATION;
import static com.yahoo.bard.webservice.config.ConfigMessageFormat.INVALID_MODULE_NAME;
import static com.yahoo.bard.webservice.config.ConfigMessageFormat.MODULE_IO_EXCEPTION;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utilities to help load resources for the system configuration.
 */
public class ModuleLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ModuleLoader.class);

    /**
     * The file name pattern describing module configuration files.
     */
    public static final String MODULE_CONFIG_FILE_NAME = "/moduleConfig.properties";

    private static final String MODULE_NAME_IS_TOO_SHORT = "Module name is too short.";

    private static final String ILLEGAL_CHARACTER_IN_MODULE_NAME = "Illegal characters '%s' in module name.";

    private final ConfigResourceLoader configResourceLoader;

    /**
     * Constructor.
     *
     * @param configResourceLoader  The configuration resource loader used to load modules
     */
    public ModuleLoader(ConfigResourceLoader configResourceLoader) {
        this.configResourceLoader = configResourceLoader;
    }

    /**
     * Get a stream of configurations in descending order of precedence given a list of dependent modules.
     *
     * @param dependentModules  The list of modules which are depended on
     *
     * @return A stream of module configurations in descending order of precedence
     */
    public Stream<Configuration> getConfigurations(List<String> dependentModules) {
        LOG.debug("Resolving dependent modules: {}", dependentModules);
        ConfigurationGraph graph = loadConfigurationGraph();

        Iterable<String> reverseList = () -> dependentModules.stream()
                .collect(Collectors.toCollection(LinkedList::new))
                .descendingIterator();

        // Because we want the configurations in precedence order, process the dependent modules from right to left,
        // deduping redundant (repeated with lower precedence) dependencies
        return StreamSupport.stream(reverseList.spliterator(), false)
                .flatMap(graph::preOrderRightToLeftTraversal)
                .distinct()
                .map(graph::getConfiguration);
    }

    /**
     * Build the graph of configurations and dependencies.
     *
     * @return A graph whose vertices are Configurations and whose edges are dependencies between them.
     *
     * @throws SystemConfigException If any errors occur while parsing the configurations into a graph.
     */
    private ConfigurationGraph loadConfigurationGraph() throws SystemConfigException {
        try {
            Map<Configuration, String> configurationFileNameMap = configResourceLoader
                    .loadResourcesWithName(MODULE_CONFIG_FILE_NAME)
                    .collect(Collectors.toMap(configResourceLoader::loadConfigFromResource, Resource::getDescription)
            );
            return new ConfigurationGraph(configurationFileNameMap, ModuleLoader::validateModuleName);

        } catch (IOException e) {
            LOG.error(MODULE_IO_EXCEPTION.logFormat(e.getMessage()));
            throw new SystemConfigException(MODULE_IO_EXCEPTION.format(e.getMessage()), e);
        }
    }

    /**
     * A method used to apply validation rules to module names found in resource property files.
     * Throws exceptions when invalid.
     *
     * @param name  The name under test.
     *
     * @throws SystemConfigException when a name fails a validation rule
     */
    public static void validateModuleName(String name) throws SystemConfigException {
        char[] nameChars = name.toCharArray();

        // Module name should not be a single character
        if (nameChars.length < 2) {
            LOG.error(INVALID_MODULE_CONFIGURATION.logFormat(MODULE_NAME_IS_TOO_SHORT, name));
            throw new SystemConfigException(INVALID_MODULE_CONFIGURATION.format(MODULE_NAME_IS_TOO_SHORT, name));
        }

        List<Character> invalidCharacters = new ArrayList<>(name.length());

        if (!Character.isJavaIdentifierStart(name.charAt(0))) {
            invalidCharacters.add(name.charAt(0));
        }

        name.substring(1).chars()
                .mapToObj(charCode -> (char) charCode)
                .filter(character -> !Character.isJavaIdentifierPart(character) && character != '-')
                .forEach(invalidCharacters::add);

        if (!invalidCharacters.isEmpty()) {
            String message = String.format(ILLEGAL_CHARACTER_IN_MODULE_NAME, invalidCharacters);
            LOG.error(INVALID_MODULE_NAME.logFormat(name, message));
            throw new SystemConfigException(INVALID_MODULE_NAME.logFormat(name, message));
        }
    }
}
