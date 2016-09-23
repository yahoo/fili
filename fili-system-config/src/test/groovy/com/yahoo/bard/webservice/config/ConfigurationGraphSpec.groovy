// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config

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
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.MODULE_F_NAME
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.MODULE_G_NAME
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.MODULE_H_NAME
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.MODULE_J_NAME
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.MODULE_MISSING_CHILD
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.MODULE_NOT_APPEARING
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.buildModule
import static com.yahoo.bard.webservice.config.ConfigurationTestUtils.namedConfigurations

import org.apache.commons.configuration.Configuration

import spock.lang.Specification
import spock.lang.Unroll

class ConfigurationGraphSpec extends Specification {

    public static final ConfigurationGraph GRAPH = new ConfigurationGraph(namedConfigurations, {})

    def "Constructor loads vertices and edges correctly"() {
        setup:
        Set<String> expectedNames = [
                MODULE_A_NAME,
                MODULE_B_NAME,
                MODULE_C_NAME,
                MODULE_D_NAME,
                MODULE_E_NAME,
                MODULE_F_NAME,
                MODULE_G_NAME,
                MODULE_H_NAME,
                MODULE_J_NAME,
                MODULE_MISSING_CHILD
        ] as Set

        Set<Configuration> expectedConfigurations =
                [A_CONFIG, B_CONFIG, C_CONFIG, D_CONFIG, E_CONFIG, F_CONFIG, G_CONFIG, H_CONFIG, J_CONFIG, MISSING_CHILD_CONFIG] as Set

        expect:
        GRAPH.moduleConfigurations.values() as Set == expectedConfigurations
        GRAPH.moduleDependencies.keySet() == expectedNames
        GRAPH.moduleDependencies.get(MODULE_A_NAME) == [MODULE_C_NAME, MODULE_B_NAME]
        GRAPH.moduleDependencies.get(MODULE_C_NAME) == []
    }

    def "Module name collision produces error"() {
        setup:
        Configuration A_DUPE_CONFIG = buildModule(MODULE_A_NAME, [MODULE_C_NAME])
        Map<Configuration, String> dupedConfigurations = [(A_CONFIG): "a", (A_DUPE_CONFIG): "x"]
        String moduleMessage = ConfigMessageFormat.MODULE_NAME_DUPLICATION.format("x", MODULE_A_NAME);

        when:
        new ConfigurationGraph(dupedConfigurations, {})

        then:
        SystemConfigException exception = thrown(SystemConfigException)
        exception.getMessage() == moduleMessage
    }

    /*
     * Modules return in precedence order from the graph.
     * The traversal is a depth first, right to left relative to the description in the module property.
     *
     * If a lists b and c as dependencies, a has precedence, followed by c, followed by b.  If b depends on d, then
     * a will have precedence, then c, then b, then d.
     */

    @Unroll
    def "Node #root resolves to path #moduleList"() {
        expect:
        GRAPH.preOrderRightToLeftTraversal(root).collect {it} == moduleList

        where:
        root          | moduleList
        MODULE_A_NAME | [MODULE_A_NAME, MODULE_C_NAME, MODULE_B_NAME, MODULE_D_NAME]
        MODULE_B_NAME | [MODULE_B_NAME, MODULE_D_NAME]
        MODULE_C_NAME | [MODULE_C_NAME]
        MODULE_D_NAME | [MODULE_D_NAME]
        MODULE_E_NAME | [MODULE_E_NAME, MODULE_C_NAME]
        MODULE_H_NAME | [MODULE_H_NAME, MODULE_A_NAME, MODULE_C_NAME, MODULE_B_NAME, MODULE_D_NAME, MODULE_E_NAME, MODULE_C_NAME]
    }

    @Unroll
    def "Visit fails on circular dependency from #root"() {
        when:
        GRAPH.preOrderRightToLeftTraversal(root).count()

        then:
        SystemConfigException exception = thrown(SystemConfigException)
        exception.getMessage() == ConfigMessageFormat.CIRCULAR_DEPENDENCY.format(duplicate, path)

        where:
        root          | duplicate     | path
        MODULE_F_NAME | MODULE_F_NAME | [MODULE_F_NAME, MODULE_G_NAME]
        MODULE_G_NAME | MODULE_G_NAME | [MODULE_G_NAME, MODULE_F_NAME]
        MODULE_J_NAME | MODULE_F_NAME | [MODULE_J_NAME, MODULE_F_NAME, MODULE_G_NAME]
    }

    def "Visit throws exception when root node doesn't exist"() {
        setup:
        String notANode = "not a node"

        when:
        GRAPH.preOrderRightToLeftTraversal(notANode)

        then:
        SystemConfigException systemConfigException = thrown(SystemConfigException)
        systemConfigException.getMessage() == ConfigMessageFormat.NO_SUCH_MODULE.format(notANode)
    }

    def "Visit throws exception when dependency link is broken"() {
        setup:
        String brokenDepedencyName = MODULE_MISSING_CHILD

        when:
        GRAPH.preOrderRightToLeftTraversal(brokenDepedencyName).collect {it}

        then:
        SystemConfigException systemConfigException = thrown(SystemConfigException)
        systemConfigException.getMessage() == ConfigMessageFormat.MISSING_DEPENDENCY.format(MODULE_NOT_APPEARING, [MODULE_MISSING_CHILD])
    }

}
