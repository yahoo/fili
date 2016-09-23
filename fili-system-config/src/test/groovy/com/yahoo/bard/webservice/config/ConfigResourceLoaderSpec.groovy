// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config

import org.apache.commons.configuration.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.core.io.Resource

import spock.lang.Specification
import spock.lang.Unroll

import java.util.regex.Matcher
import java.util.stream.Collectors

/**
 *  Test low level resource loading works using module files.
 */
class ConfigResourceLoaderSpec extends Specification {

    public static final String MODULE_NAME_KEY = "moduleName"
    public static final String MODULE_RESOURCE_NAME = "/moduleConfig.properties"

    def moduleFilePattern = (/.*(fili-system-config-test\d).*/)

    ConfigResourceLoader configResourceLoader = new ConfigResourceLoader()

    def "test sample data exists" () {
        expect:
        new ClassPathResource("jar1-contents/moduleConfig.properties").exists()
        new ClassPathResource("jar2-contents/moduleConfig.properties").exists()
    }

    /**
     * Process a regular expression and a source string into a List of matches broken out into Lists of groups matched.
     *
     * @param regEx  A regular expression
     * @param source  A source string for matches
     *
     * @return  The match data broken out as a list of lists of matching groups
     */
    List<List<String>> readMatches(String regEx, String source) {
        List<List<String>> result = []
        Matcher matcher = source =~ regEx
        while (matcher.find()) {
            // Get the groups and add them to the result
            result << (1..matcher.groupCount()).collect { matcher.group(it as int) }
        }
        return result
    }

    def "test load resources with name finds both module resources from classpath"() {
        setup:
        Resource jar1 = new ClassPathResource("jars/fili-system-config-test1.jar");
        Resource jar2 = new ClassPathResource("jars/fili-system-config-test2.jar");

        assert jar1.exists()
        assert jar2.exists()

        Set<Resource> resources =  configResourceLoader.loadResourcesWithName(MODULE_RESOURCE_NAME).collect(Collectors.toSet())
        List<URI> expectedResources = [jar1, jar2].collect { it.URI }

        List<String> actual = resources.collect { readMatches(moduleFilePattern, "$it") }.flatten()
        List<String> expected = expectedResources.collect { readMatches(moduleFilePattern, "$it") }.flatten()

        expect:
        actual.containsAll(expected)
        expected.containsAll(actual)
    }

    def "Load configurations by name works"() {
        setup:
        Set<String> expectedModuleNames = ["fili-system-configuration-test1", "fili-system-configuration-test2"] as Set
        List<Configuration> configurations = configResourceLoader.loadConfigurations(MODULE_RESOURCE_NAME)

        expect:
        configurations.collect { it.getString(MODULE_NAME_KEY)} as Set == expectedModuleNames
    }

    @Unroll
    def "Load config from resources works"() {
        setup:
        Resource resource = new ClassPathResource(configName)

        expect:
        configResourceLoader.loadConfigFromResource(resource).getString(MODULE_NAME_KEY) == moduleName

        where:
        configName                              | moduleName
        "jar1-contents/moduleConfig.properties" | "fili-system-configuration-test1"
        "jar2-contents/moduleConfig.properties" | "fili-system-configuration-test2"
    }

    def "test load resources with name finds both non module resources from classpath"() {
        setup:
        Set<String> allNames =  configResourceLoader.loadConfigurations("NotAModule.txt").collect {it.getString("name")} as Set
        Set<String> noJarNames =  configResourceLoader.loadConfigurationsNoJars("NotAModule.txt").collect {it.getString("name")} as Set

        expect:
        allNames == ["NotAModuleInJar", "NotAModuleNoJar"] as Set
        noJarNames == ["NotAModuleNoJar"] as Set
    }
}
