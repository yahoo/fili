// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

import static com.yahoo.bard.webservice.config.ConfigMessageFormat.TOO_MANY_APPLICATION_CONFIGS;
import static com.yahoo.bard.webservice.config.ConfigMessageFormat.TOO_MANY_USER_CONFIGS;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class to hold and fetch configuration values from the environment, the system, user, application, module and
 * library configurations.
 * <p>
 * LayeredFileSystemConfig uses a layered model with the highest priority granted to environment variables, followed
 * by system properties, then user configuration, application configuration, module configurations and then core
 * library default configuration.
 * <p>
 * LayeredFileSystemConfig also uses a Properties resource to allow runtime override of configured behavior.
 */
public class LayeredFileSystemConfig implements SystemConfig {

    private static final Logger LOG = LoggerFactory.getLogger(LayeredFileSystemConfig.class);

    /**
     * The resource path for local user override of application and default properties.
     */
    private static final String USER_CONFIG_FILE_NAME = "/userConfig.properties";

    /**
     * The resource path for configuring properties within an application.
     */
    private static final String APPLICATION_CONFIG_FILE_NAME = "/applicationConfig.properties";

    /**
     * The resource path for test environment properties.
     */
    private static final String TEST_CONFIG_FILE_NAME = "/testApplicationConfig.properties";

    /**
     * The composite configuration containing the layered properties and values.
     */
    private final CompositeConfiguration masterConfiguration;

    /**
     * Runtime properties are used as runtime overrides of load time configuration.
     */
    private final Properties runtimeProperties;

    /**
     * Build a Layered File System Configuration, using first the environment and an application configuration source,
     * then drill down into available modules and load each of them in package dependency order.
     */
    public LayeredFileSystemConfig() {
        masterConfiguration = new CompositeConfiguration();
        masterConfiguration.setThrowExceptionOnMissing(true);
        runtimeProperties = new Properties();

        try {
            // Loader pulls resources in from class path locations.
            ConfigResourceLoader loader = new ConfigResourceLoader();

            // User configuration provides overrides for configuration on a specific environment or specialized role
            List<Configuration> userConfig = loader.loadConfigurations(USER_CONFIG_FILE_NAME);
            if (userConfig.size() > 1) {
                List<Resource> resources = loader.loadResourcesWithName(USER_CONFIG_FILE_NAME)
                        .collect(Collectors.toList());
                LOG.error(TOO_MANY_USER_CONFIGS.logFormat(resources.toString()));
                throw new SystemConfigException(TOO_MANY_USER_CONFIGS.format(resources.size()));
            }

            // Test application configuration provides overrides for configuration in a testing environment
            List<Configuration> testApplicationConfig = loader.loadConfigurationsNoJars(TEST_CONFIG_FILE_NAME);

            // Application configuration defines configuration at an application level for a bard instance
            List<Configuration> applicationConfig = loader.loadConfigurations(APPLICATION_CONFIG_FILE_NAME);
            if (applicationConfig.size() > 1) {
                List<Resource> resources = loader.loadResourcesWithName(APPLICATION_CONFIG_FILE_NAME)
                        .collect(Collectors.toList());
                LOG.error(TOO_MANY_APPLICATION_CONFIGS.logFormat(resources.toString()));
                throw new SystemConfigException(TOO_MANY_APPLICATION_CONFIGS.format(resources.size()));
            }

            // Use PropertiesConfiguration to hold environment variables to ensure same behavior as properties files
            PropertiesConfiguration environmentConfiguration = new PropertiesConfiguration();

            for (Map.Entry entry : System.getenv().entrySet()) {
                // addProperty will parse string as list with delimiter set
                environmentConfiguration.addProperty(entry.getKey().toString(), entry.getValue());
            }

            // Environment config has higher priority than java system properties
            // Java system properties have higher priority than file based configuration
            // Also, a runtime map is maintained to support on-the-fly configuration changes

            // Load the rest of the config "top down" through the layers, in highest to lowest precedence
            Stream.of(
                    Stream.of(new MapConfiguration(runtimeProperties)),
                    Stream.of(environmentConfiguration),
                    Stream.of(new SystemConfiguration()),
                    userConfig.stream(),
                    testApplicationConfig.stream(),
                    applicationConfig.stream()
            )
                    .flatMap(Function.identity())
                    .filter(Objects::nonNull)
                    .forEachOrdered(masterConfiguration::addConfiguration);

            // Use the config which has been loaded to identify module dependencies
            List<String> dependentModules = masterConfiguration.getList(
                    ConfigurationGraph.DEPENDENT_MODULE_KEY,
                    Collections.<String>emptyList()
            ).stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());

            // Add module dependencies to the master configuration
            new ModuleLoader(loader).getConfigurations(dependentModules).forEach(
                    masterConfiguration::addConfiguration
            );
        } catch (IOException e) {
            throw new SystemConfigException(e);
        }
    }

    @Override
    public CompositeConfiguration getMasterConfiguration() {
        return masterConfiguration;
    }

    @Override
    public Properties getRuntimeProperties() {
        return runtimeProperties;
    }
}
