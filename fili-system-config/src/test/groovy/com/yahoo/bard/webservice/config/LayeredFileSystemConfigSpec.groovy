// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config

import spock.lang.Requires
import spock.lang.Unroll

/**
 * Test Layered File configuration spec
 */
class LayeredFileSystemConfigSpec extends SystemConfigSpec {

    private static final MODULE_1_PREFIX = "fili-system-configuration-test1";
    private static final MODULE_2_PREFIX = "fili-system-configuration-test2";
    private static final SHARED_PREFIX = "fili-shared";
    private static final APPLICATION_PREFIX = "application_scope";

    public static final String TEST_PROPERTY = "__test_property"
    private static final SHARED_PROPERTY = SHARED_PREFIX + TEST_PROPERTY

    private static final IS_TEST_PROPERTY = "is_test_application"

    private static final String ENVIRONMENT_PROPERTY_KEY = "JAVA_HOME"

    // This environment is set in build container for testing purposes only
    private static final String ENVIRONMENT_LIST_PROPERTY_KEY = "FILI_TEST_LIST"

    private SystemConfig systemConfig = new LayeredFileSystemConfig()

    @Override
    SystemConfig getTestSystemConfig() {
        new LayeredFileSystemConfig()
    }

    @Requires({System.getenv(ENVIRONMENT_PROPERTY_KEY) != null})
    def "An environment property is visible"() {
        expect:
        systemConfig.getStringProperty(ENVIRONMENT_PROPERTY_KEY)
    }

    def "Module properties are visible"() {
        expect:
        systemConfig.getStringProperty(MODULE_1_PREFIX + TEST_PROPERTY) == "test1"
        systemConfig.getStringProperty(MODULE_2_PREFIX + TEST_PROPERTY) == "test2"
    }

    @Requires({System.getenv(ENVIRONMENT_LIST_PROPERTY_KEY) != null})
    def "A comma separated list environment variable gets parsed correctly as a list"() {
        setup:
        List<String> expectedList = System.getenv(ENVIRONMENT_LIST_PROPERTY_KEY).split(",").toList()

        expect:
        systemConfig.getListProperty(ENVIRONMENT_LIST_PROPERTY_KEY) == expectedList
    }

    @Requires({System.getenv(ENVIRONMENT_PROPERTY_KEY) != null})
    def "Precedence: A runtime property is higher than an environment property"() {
        setup:
        String newValue = "New Value"
        String originalValue = systemConfig.getRuntimeProperties().setProperty(ENVIRONMENT_PROPERTY_KEY, newValue)

        expect:
        systemConfig.getStringProperty(ENVIRONMENT_PROPERTY_KEY) == newValue

        cleanup:
        systemConfig.resetProperty(ENVIRONMENT_PROPERTY_KEY, originalValue)
    }

    @Requires({System.getenv(ENVIRONMENT_PROPERTY_KEY) != null})
    def "Precedence: An environment property is higher than a system property"() {
        setup:
        String badValue = "Bad Value"
        String original = System.getProperty(ENVIRONMENT_PROPERTY_KEY);
        System.setProperty(ENVIRONMENT_PROPERTY_KEY, badValue)

        expect:
        systemConfig.getStringProperty(ENVIRONMENT_PROPERTY_KEY) != badValue

        cleanup:
        if (original == null) {
            System.clearProperty(ENVIRONMENT_PROPERTY_KEY)
        } else {
            System.setProperty(ENVIRONMENT_PROPERTY_KEY, original)
        }
    }

    def "Precedence: A system property is higher priority than a test, application or module property"() {
        given: "A system property with the same name as a default property but a different value"
        String moduleKey = MODULE_1_PREFIX + TEST_PROPERTY
        String applicationKey = "application_scope_only" + TEST_PROPERTY

        String initialModuleValue = systemConfig.getStringProperty(moduleKey)
        String initialApplicationValue = systemConfig.getStringProperty(applicationKey)
        boolean originalTestState = systemConfig.getBooleanProperty(IS_TEST_PROPERTY)

        String differentValue = "Some Very Different Value!"
        systemConfig.setProperty(moduleKey, differentValue)
        systemConfig.setProperty(applicationKey, differentValue)
        systemConfig.setProperty(IS_TEST_PROPERTY, "false")

        expect: "Different value is different"
        initialModuleValue != differentValue
        initialApplicationValue != differentValue
        originalTestState

        and: "The system-specified value is the one we get"
        differentValue == systemConfig.getStringProperty(moduleKey)
        differentValue == systemConfig.getStringProperty(applicationKey)
        ! systemConfig.getBooleanProperty(IS_TEST_PROPERTY)

        cleanup:
        systemConfig.clearProperty(moduleKey)
        systemConfig.clearProperty(applicationKey)
        systemConfig.clearProperty(IS_TEST_PROPERTY)
    }


    def "Precedence: An test property is higher than an application property "() {
        expect: "Application property used in modules is visible"
        systemConfig.getBooleanProperty(IS_TEST_PROPERTY)
    }

    def "Precedence: An application property is higher than a module property"() {
        expect: "Application property used in modules is visible"
        systemConfig.getStringProperty(APPLICATION_PREFIX + TEST_PROPERTY) == "applicationLevel"
    }

    @Unroll
    def "Precedence: Module dependency order is respected: #dependentList"() {
        setup:
        def original = System.setProperty(ConfigurationGraph.DEPENDENT_MODULE_KEY, dependentList)
        SystemConfig config = new LayeredFileSystemConfig()

        expect:
        config.getStringProperty(SHARED_PROPERTY) == expected

        cleanup:
        if (original != null) {
            System.setProperty(ConfigurationGraph.DEPENDENT_MODULE_KEY, original)
        } else {
            System.clearProperty(ConfigurationGraph.DEPENDENT_MODULE_KEY)
        }

        where:
        dependentList                                                      | expected
        "fili-system-configuration-test1, fili-system-configuration-test2" | "shared2"
        "fili-system-configuration-test2, fili-system-configuration-test1" | "shared1"
    }
}
