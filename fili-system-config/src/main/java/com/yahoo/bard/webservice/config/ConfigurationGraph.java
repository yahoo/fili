// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

import static com.yahoo.bard.webservice.config.ConfigMessageFormat.CIRCULAR_DEPENDENCY;
import static com.yahoo.bard.webservice.config.ConfigMessageFormat.MISSING_DEPENDENCY;
import static com.yahoo.bard.webservice.config.ConfigMessageFormat.MODULE_DEPENDS_ON_MESSAGE;
import static com.yahoo.bard.webservice.config.ConfigMessageFormat.MODULE_FOUND_MESSAGE;
import static com.yahoo.bard.webservice.config.ConfigMessageFormat.MODULE_NAME_DUPLICATION;
import static com.yahoo.bard.webservice.config.ConfigMessageFormat.MODULE_NAME_MISSING;
import static com.yahoo.bard.webservice.config.ConfigMessageFormat.NO_SUCH_MODULE;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Graph representing a set of module configurations with dependencies mapped by name inside the configuration.
 */
public class ConfigurationGraph {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationGraph.class);

    /**
     * The special property which uniquely identifies the name of the module.
     */
    public static final String MODULE_NAME_KEY = "moduleName";

    /**
     * The special property which identifies dependent modules.
     */
    public static final String DEPENDENT_MODULE_KEY = "moduleDependencies";

    /**
     * Map of the names of modules to the backing configuration files.
     * These serve as the vertices of the graph.
     */
    private final Map<String, Configuration> moduleConfigurations;

    /**
     * Map of the names of modules to the names of the modules they depend upon.
     * Store in reverse order from the property to support right to left traversal.  These serve as the edges of the
     * graph.
     */
    private final Map<String, List<String>> moduleDependencies;

    /**
     * Create a configuration graph from a collection of Configuration, resource name pairs and a name validation
     * function.
     *
     * @param configurationNamePairs  A map whose keys are configurations and values are the resource names use to
     * report errors while processing configurations.
     * @param nameValidator  A function which throws exceptions on module names which are not valid.
     */
    public ConfigurationGraph(Map<Configuration, String> configurationNamePairs, Consumer<String> nameValidator) {
        moduleConfigurations = new HashMap<>();
        moduleDependencies = new LinkedHashMap<>();

        for (Map.Entry<Configuration, String> configEntry : configurationNamePairs.entrySet()) {
            addVertex(configEntry.getKey(), configEntry.getValue(), nameValidator);
        }
    }

    /**
     * Take a configuration and if it is a valid module, load it into the moduleConfigurations map and load it's
     * dependency moduleDependencies.
     *
     * @param configuration  A configuration which may be a module
     * @param configName  The resource name for that configuration
     * @param nameValidator  A function which throws exceptions on module names which are not valid.
     */
    private void addVertex(Configuration configuration, String configName, Consumer<String> nameValidator) {
        if (!configuration.containsKey(MODULE_NAME_KEY)) {
            // This may be the result of another library using one of our configuration names
            LOG.warn(MODULE_NAME_MISSING.logFormat(configName));
            return;
        }
        String moduleName = configuration.getString(MODULE_NAME_KEY);
        nameValidator.accept(moduleName);

        LOG.debug(MODULE_FOUND_MESSAGE.logFormat(moduleName, configName));
        if (moduleConfigurations.containsKey(moduleName)) {
            LOG.error(MODULE_NAME_DUPLICATION.format(configName, moduleName));
            throw new SystemConfigException(MODULE_NAME_DUPLICATION.format(configName, moduleName));
        }
        moduleConfigurations.put(moduleName, configuration);

        List<String> dependencies = configuration.getList(DEPENDENT_MODULE_KEY, Collections.<String>emptyList())
                .stream()
                .map(Object::toString)
                .collect(Collectors.toList());

        // later dependencies have higher precedence.  Store moduleDependencies in precedence order descending
        Collections.reverse(dependencies);
        LOG.debug(MODULE_DEPENDS_ON_MESSAGE.logFormat(moduleName, dependencies));

        moduleDependencies.put(moduleName, dependencies);
    }

    /**
     * Return the configuration corresponding to a module name.
     *
     * @param nodeName  The module name for the graph
     *
     * @return The configuration of a module
     */
    public Configuration getConfiguration(String nodeName) {
        return moduleConfigurations.get(nodeName);
    }

    /**
     * Find the prioritized stream of configurations for a given module (inclusive of the module itself).
     *
     * @param nodeName  The name of the initial module whose dependency should be resolved
     *
     * @return A list of modules returned in the order of increasing precedence
     *
     * @throws SystemConfigException if the graph can't be resolved
     */
    public Stream<String> preOrderRightToLeftTraversal(String nodeName) throws SystemConfigException {
        if (!moduleConfigurations.containsKey(nodeName)) {
            LOG.error(NO_SUCH_MODULE.logFormat(nodeName));
            throw new SystemConfigException(NO_SUCH_MODULE.format(nodeName));
        }
        return preOrderRightToLeftTraversal(nodeName, new ArrayList<>());
    }

    /**
     * Find the prioritized stream of configurations for a given module (inclusive of the module itself).
     *
     * @param nodeName  The name of the initial module whose dependencies to load (inclusively)
     * @param path  The list of nodes back to the root of the tree parse
     *
     * @return  A list of modules returned in the order of increasing precedence
     *
     * @throws SystemConfigException if there is a broken or circular dependency link
     */
    protected Stream<String> preOrderRightToLeftTraversal(String nodeName, List<String> path)
            throws SystemConfigException {
        if (!moduleConfigurations.containsKey(nodeName)) {
            LOG.error(MISSING_DEPENDENCY.logFormat(nodeName, path));
            throw new SystemConfigException(MISSING_DEPENDENCY.format(nodeName, path));
        }

        if (path.contains(nodeName)) {
            LOG.error(CIRCULAR_DEPENDENCY.logFormat(nodeName, path));
            throw new SystemConfigException(CIRCULAR_DEPENDENCY.format(nodeName, path));
        }
        List<String> pathLocal = new ArrayList<>(path);
        pathLocal.add(nodeName);
        Stream<String> childrenStream = moduleDependencies.get(nodeName).stream()
                .flatMap(childNode -> preOrderRightToLeftTraversal(childNode, pathLocal));
        return Stream.concat(Stream.of(nodeName), childrenStream);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof ConfigurationGraph)) { return false; }

        final ConfigurationGraph that = (ConfigurationGraph) o;

        if (!moduleConfigurations.equals(that.moduleConfigurations)) { return false; }
        return moduleDependencies.equals(that.moduleDependencies);

    }

    @Override
    public int hashCode() {
        int result = moduleConfigurations.hashCode();
        result = 31 * result + moduleDependencies.hashCode();
        return result;
    }
}
