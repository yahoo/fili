// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.systemconfig

import static com.yahoo.bard.webservice.config.ModuleLoader.MODULE_CONFIG_FILE_NAME

import com.yahoo.bard.webservice.config.ConfigResourceLoader
import com.yahoo.bard.webservice.config.SystemConfigProvider

import org.apache.commons.configuration.Configuration

import spock.lang.Specification
import spock.lang.Unroll
/**
 * An abstract base TCK test for modules.
 * Usage:  Subclass this spec, in test/groovy/ packages, implementing getModuleName() with your module's name.
 */
abstract class ModuleSetupSpec extends Specification {

    public static final String MODULE_NAME_KEY = "moduleName"

    /**
     * Implement this test by supplying a subclass that provides the name of the module
     *
     * @return  The name of the module as configured in moduleConfig.properties
     */
    abstract String getModuleName()

    @Unroll
    def "#moduleName does not break SystemConfig"() {
        expect:
        SystemConfigProvider.instance.runtimeProperties.getOrDefault("Does getting a property throw an exception?", "foo") == "foo"

        where:
        moduleName << [getModuleName()]
    }

    @Unroll
    def "ConfigResourceLoader discovers #moduleName"() {
        setup:
        List<Configuration> configurations = new ConfigResourceLoader().loadConfigurations(MODULE_CONFIG_FILE_NAME)

        expect:
        configurations.stream().map( {it.getString(MODULE_NAME_KEY)}).anyMatch({it.equals(getModuleName())})

        where:
        moduleName << [getModuleName()]
    }
}
