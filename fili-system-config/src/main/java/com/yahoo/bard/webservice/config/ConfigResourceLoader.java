// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

import static com.yahoo.bard.webservice.config.ConfigMessageFormat.CONFIGURATION_LOAD_ERROR;
import static com.yahoo.bard.webservice.config.ConfigMessageFormat.RESOURCE_LOAD_MESSAGE;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utilities to help load resources for the system configuration.
 */
public class ConfigResourceLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigResourceLoader.class);

    protected static final String RESOURCE_LOADER_PREFIX = "classpath*:";

    private final PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    /**
     * Use a string pattern to load configurations matching a resource name from the class path and parse into
     * Configuration objects.
     *
     * @param name  The class path address of a resource ('/foo' means a resource named foo in the default package)
     *
     * @return A list of configurations corresponding to the matching class path resource
     *
     * @throws IOException if any resources cannot be read from the class path successfully.
     */
    public List<Configuration> loadConfigurations(String name) throws IOException {
        return loadResourcesWithName(name).map(this::loadConfigFromResource).collect(Collectors.toList());
    }

    /**
     * Use a string path to load configurations matching a resource name from the class path which are not from
     * jars into Configuration objects.
     * <p>
     * This differentiation generally provides for only the local src directories to supply the resource instances,
     * preventing injection of unwanted resources from class path entries.
     *
     * @param name  The class path address of a resource ('/foo' means a resource named foo in the default package)
     *
     * @return A list of configurations corresponding to the matching class path resource
     *
     * @throws IOException if any resources cannot be read from the class path successfully.
     */
    public List<Configuration> loadConfigurationsNoJars(String name) throws IOException {
        return loadResourcesWithName(name)
                .filter(this::isResourceNotAJar)
                .map(this::loadConfigFromResource)
                .collect(Collectors.toList());
    }

    /**
     * Use a string to load a stream of all resources from the class path which match a given name.
     *
     * @param  name The class path address of a resource ('/foo' means a resource named foo in the default package)
     *
     * @return A stream of all class path resources corresponding to a particular name
     *
     * @throws IOException if any resources cannot be read from the class path successfully.
     */
    public Stream<Resource> loadResourcesWithName(String name) throws IOException {
        String resourceName = RESOURCE_LOADER_PREFIX + name;
        LOG.debug("Loading resources named '{}'", resourceName);
        return Arrays.stream(resolver.getResources(resourceName))
                .peek(it -> LOG.debug(RESOURCE_LOAD_MESSAGE.logFormat(name, it)));
    }

    /**
     * Build a configuration object from a resource, processing it as a properties file.
     *
     * @param resource  The resource referring to a properties file
     *
     * @return a Configuration object containing a properties configuration
     */
    public Configuration loadConfigFromResource(Resource resource) {
        PropertiesConfiguration result = new PropertiesConfiguration();
        try {
            result.load(resource.getInputStream());
            return result;
        } catch (ConfigurationException | IOException e) {
            String message = CONFIGURATION_LOAD_ERROR.format(resource.getFilename());
            LOG.error(message, e);
            throw new SystemConfigException(message, e);
        }
    }

    /**
     * A simple predicate that is true if a resource is not from a jar.
     *
     * @param resource  the Resource under test
     *
     * @return true if the resource is not from a jar
     */
    public boolean isResourceNotAJar(Resource resource) {
        try {
            return !resource.getURI().getScheme().equals("jar");
        } catch (IOException ignored) {
            // If the resource doesn't parse cleanly as a URI, it's not a jar
            return true;
        }
    }
}
