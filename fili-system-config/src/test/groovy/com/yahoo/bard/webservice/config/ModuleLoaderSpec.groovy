// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config

import static com.yahoo.bard.webservice.config.ConfigurationGraph.MODULE_NAME_KEY
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.A_CONFIG
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.B_CONFIG
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.C_CONFIG
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.D_CONFIG
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.E_CONFIG
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.F_CONFIG
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.G_CONFIG
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.H_CONFIG
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.J_CONFIG
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.MISSING_CHILD_CONFIG
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.MODULE_A_NAME
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.MODULE_B_NAME
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.MODULE_C_NAME
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.MODULE_D_NAME
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.MODULE_E_NAME
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.MODULE_H_NAME
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.namedConfigurations
import static com.yahoo.bard.webservice.config.ModuleLoader.ILLEGAL_CHARACTER_IN_MODULE_NAME
import static com.yahoo.bard.webservice.config.ModuleLoader.MODULE_NAME_IS_TOO_SHORT

import org.apache.commons.configuration.Configuration
import org.springframework.core.io.Resource

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Test for the ModuleLoader code
 */
class ModuleLoaderSpec extends Specification{

    public static final String RESOURCE_NAME_A = "a"
    public static final String RESOURCE_NAME_B = "b"
    public static final String RESOURCE_NAME_C = "c"
    public static final String RESOURCE_NAME_D = "d"
    public static final String RESOURCE_NAME_E = "e"
    public static final String RESOURCE_NAME_F = "f"
    public static final String RESOURCE_NAME_G = "g"
    public static final String RESOURCE_NAME_H = "h"
    public static final String RESOURCE_NAME_J = "j"
    public static final String RESOURCE_NAME_MISSING_CHILD = "missing-child"

    Resource RESOURCE_A = Mock(Resource)
    Resource RESOURCE_B = Mock(Resource)
    Resource RESOURCE_C = Mock(Resource)
    Resource RESOURCE_D = Mock(Resource)
    Resource RESOURCE_E = Mock(Resource)
    Resource RESOURCE_F = Mock(Resource)
    Resource RESOURCE_G = Mock(Resource)
    Resource RESOURCE_H = Mock(Resource)
    Resource RESOURCE_J = Mock(Resource)
    Resource RESOURCE_MISSING_CHILD = Mock(Resource)

    Map<Resource, Configuration> resourceConfigurationMap = [
            (RESOURCE_A): A_CONFIG,
            (RESOURCE_B): B_CONFIG,
            (RESOURCE_C): C_CONFIG,
            (RESOURCE_D): D_CONFIG,
            (RESOURCE_E): E_CONFIG,
            (RESOURCE_F): F_CONFIG,
            (RESOURCE_G): G_CONFIG,
            (RESOURCE_H): H_CONFIG,
            (RESOURCE_J): J_CONFIG,
            (RESOURCE_MISSING_CHILD): MISSING_CHILD_CONFIG
    ]

    ConfigResourceLoader resourceLoader

    def setup() {
        RESOURCE_A.getFilename() >> RESOURCE_NAME_A
        RESOURCE_B.getFilename() >> RESOURCE_NAME_B
        RESOURCE_C.getFilename() >> RESOURCE_NAME_C
        RESOURCE_D.getFilename() >> RESOURCE_NAME_D
        RESOURCE_E.getFilename() >> RESOURCE_NAME_E
        RESOURCE_F.getFilename() >> RESOURCE_NAME_F
        RESOURCE_G.getFilename() >> RESOURCE_NAME_G
        RESOURCE_H.getFilename() >> RESOURCE_NAME_H
        RESOURCE_J.getFilename() >> RESOURCE_NAME_J
        RESOURCE_A.getDescription() >> RESOURCE_NAME_A
        RESOURCE_B.getDescription() >> RESOURCE_NAME_B
        RESOURCE_C.getDescription() >> RESOURCE_NAME_C
        RESOURCE_D.getDescription() >> RESOURCE_NAME_D
        RESOURCE_E.getDescription() >> RESOURCE_NAME_E
        RESOURCE_F.getDescription() >> RESOURCE_NAME_F
        RESOURCE_G.getDescription() >> RESOURCE_NAME_G
        RESOURCE_H.getDescription() >> RESOURCE_NAME_H
        RESOURCE_J.getDescription() >> RESOURCE_NAME_J
        RESOURCE_MISSING_CHILD.getFilename() >> RESOURCE_NAME_MISSING_CHILD
        RESOURCE_MISSING_CHILD.getDescription() >> RESOURCE_NAME_MISSING_CHILD

        resourceLoader = Mock(ConfigResourceLoader)
        resourceLoader.loadResourcesWithName(_) >> resourceConfigurationMap.keySet().stream()
    }

    @Unroll
    def "Module getConfigurations post order searches with deduping from dependencies: #depends "() {
        setup:
        ModuleLoader moduleLoader = new ModuleLoader(resourceLoader)
        resourceLoader.loadConfigFromResource(_) >> {Resource resource -> resourceConfigurationMap.get(resource)}

        when:
        List<Configuration> actualConfigurations = moduleLoader.getConfigurations(depends).collect()

        then:
        // Added this test for readability or errors, Configuration has a very unhelpful toString
        actualConfigurations.collect { it.getString(MODULE_NAME_KEY) } == configList.collect { it.getString(MODULE_NAME_KEY) }
        actualConfigurations == configList

        where:
        depends                        | configList
        // These retest the singleton tests from ConfigurationGraphSpec
        [MODULE_A_NAME]                | [A_CONFIG, C_CONFIG, B_CONFIG, D_CONFIG]
        [MODULE_B_NAME]                | [B_CONFIG, D_CONFIG]
        [MODULE_C_NAME]                | [C_CONFIG]
        [MODULE_D_NAME]                | [D_CONFIG]
        [MODULE_E_NAME]                | [E_CONFIG, C_CONFIG]
        [MODULE_H_NAME]                | [H_CONFIG, A_CONFIG, C_CONFIG, B_CONFIG, D_CONFIG, E_CONFIG]
        // Now test combinations
        [MODULE_A_NAME, MODULE_A_NAME] | [A_CONFIG, C_CONFIG, B_CONFIG, D_CONFIG]
        [MODULE_B_NAME, MODULE_C_NAME] | [C_CONFIG, B_CONFIG, D_CONFIG]
        [MODULE_C_NAME, MODULE_B_NAME] | [B_CONFIG, D_CONFIG, C_CONFIG]
        [MODULE_A_NAME, MODULE_E_NAME] | [E_CONFIG, C_CONFIG, A_CONFIG, B_CONFIG, D_CONFIG]
        [MODULE_E_NAME, MODULE_A_NAME] | [A_CONFIG, C_CONFIG, B_CONFIG, D_CONFIG, E_CONFIG]
    }

    def "Module loader spec correctly builds configuration graph from resource list "() {
        setup:
        resourceLoader.loadConfigFromResource(_) >> {Resource resource -> resourceConfigurationMap.get(resource)}

        expect:
        new ModuleLoader(resourceLoader).loadConfigurationGraph() == new ConfigurationGraph(namedConfigurations, {})
    }

    @Unroll
    def "#moduleName too short creates validation exception with text: #message"() {
        when:
        ModuleLoader.validateModuleName(moduleName)

        then:
        SystemConfigException systemConfigException = thrown(SystemConfigException)
        systemConfigException.getMessage().contains(String.format(message))

        where:
        moduleName | message
        ""         | MODULE_NAME_IS_TOO_SHORT
        "a"        | MODULE_NAME_IS_TOO_SHORT
    }

    @Unroll
    def "#moduleName creates validation exception with text: #message"() {
        when:
        ModuleLoader.validateModuleName(moduleName)

        then:
        SystemConfigException systemConfigException = thrown(SystemConfigException)
        systemConfigException.getMessage().contains(String.format(message, args))

        where:
        moduleName | message                          | args
        "-as"      | ILLEGAL_CHARACTER_IN_MODULE_NAME | ['-' as char]
        "-as*"     | ILLEGAL_CHARACTER_IN_MODULE_NAME | ['-' as char, '*' as char]
        "as-+"     | ILLEGAL_CHARACTER_IN_MODULE_NAME | ['+' as char]
        "a s"      | ILLEGAL_CHARACTER_IN_MODULE_NAME | [' ' as char]
        "as\n"     | ILLEGAL_CHARACTER_IN_MODULE_NAME | ['\n' as char]
    }

    @Unroll
    def "#moduleName is valid"() {
        when:
        ModuleLoader.validateModuleName(moduleName)

        then:
        noExceptionThrown()

        where:
        moduleName << ["abc", "a-b", "asdbasd-asdasd", "asdasd---"]
    }
}
