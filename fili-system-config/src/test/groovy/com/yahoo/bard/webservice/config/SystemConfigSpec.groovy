// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

abstract class SystemConfigSpec extends Specification {

    @Shared SystemConfig systemConfig = getTestSystemConfig()

    private static final String MISSING_PROPERTY_KEY = "MISSING KEY"

    private static final String STRING_PROPERTY_KEY = "string_property_key"
    private static final String STRING_PROPERTY_VALUE = "forty-two"
    private static final String STRING_VALUE = "forty-two"
    private static final String STRING_DEFAULT_VALUE = "seven"

    private static final String INT_PROPERTY_KEY = "int_property_key"
    private static final String INT_PROPERTY_VALUE = "42"
    private static final Integer INT_VALUE = 42
    private static final Integer INT_DEFAULT_VALUE = 7

    private static final String LONG_PROPERTY_KEY = "long_property_key"
    private static final String LONG_PROPERTY_VALUE = "42"
    private static final Long LONG_VALUE = 42L
    private static final Long LONG_DEFAULT_VALUE = 7L

    private static final String FLOAT_PROPERTY_KEY = "float_property_key"
    private static final String FLOAT_PROPERTY_VALUE = "42.0"
    private static final Float FLOAT_VALUE = 42.0F
    private static final Float FLOAT_DEFAULT_VALUE = 7.0F

    private static final String DOUBLE_PROPERTY_KEY = "double_property_key"
    private static final String DOUBLE_PROPERTY_VALUE = "42.0"
    private static final Double DOUBLE_VALUE = 42.0D
    private static final Double DOUBLE_DEFAULT_VALUE = 7.0D

    private static final String BOOLEAN_PROPERTY_KEY = "boolean_property_key"
    private static final String BOOLEAN_PROPERTY_VALUE = "true"
    private static final Boolean BOOLEAN_VALUE = true
    private static final Boolean BOOLEAN_DEFAULT_VALUE = true

    private static final String LIST_PROPERTY_KEY = "list_property_key"
    private static final String LIST_PROPERTY_VALUE = "listA, listB"
    private static final List<String> LIST_VALUE = ["listA", "listB"]
    private static final List<String> LIST_DEFAULT_VALUE = ["list1", "list2"]

    abstract SystemConfig getTestSystemConfig();

    def setupSpec() {
        // Set up the known values
        systemConfig.setProperty(STRING_PROPERTY_KEY, STRING_PROPERTY_VALUE)
        systemConfig.setProperty(INT_PROPERTY_KEY, INT_PROPERTY_VALUE)
        systemConfig.setProperty(LONG_PROPERTY_KEY, LONG_PROPERTY_VALUE)
        systemConfig.setProperty(FLOAT_PROPERTY_KEY, FLOAT_PROPERTY_VALUE)
        systemConfig.setProperty(DOUBLE_PROPERTY_KEY, DOUBLE_PROPERTY_VALUE)
        systemConfig.setProperty(BOOLEAN_PROPERTY_KEY, BOOLEAN_PROPERTY_VALUE)
        systemConfig.setProperty(LIST_PROPERTY_KEY, LIST_PROPERTY_VALUE)

        // Make sure the missing one is missing
        assert ! systemConfig.masterConfiguration.containsKey(MISSING_PROPERTY_KEY)
    }

    @Unroll
    def "Reading a #propertyType property gives the property value"() {
        expect: "We read a property that exists, we get the value"
        value == systemConfig."get${propertyType}Property"(property)

        where:
        [propertyType, property, _, value] << getTestIterationData()
    }

    @Unroll
    def "Reading a missing #propertyType property throws an exception except for lists"() {
        when: "We read a property that doesn't exist"
        systemConfig."get${propertyType}Property"(MISSING_PROPERTY_KEY)

        then: "A SystemConfigException is thrown"
        thrown SystemConfigException

        where:
        [propertyType] << getTestIterationData(List: true)
    }

    @Unroll
    def "Reading a #propertyType property that isn't a #propertyType throws an exception except for strings and lists"() {
        when: "We read a property that exists, but isn't of the right type"
        systemConfig."get${propertyType}Property"(unconvertible)

        then: "A SystemConfigException is thrown"
        thrown SystemConfigException

        where:
        [propertyType, _, _, _, _, unconvertible] << getTestIterationData(String: true, List: true)
    }

    @Unroll
    def "Reading a #propertyType property with a default gives the property value"() {
        expect: "We read a property that doesn't exist with a default, we get the default"
        value == systemConfig."get${propertyType}Property"(property, defaultValue)

        where:
        [propertyType, property, _, value, defaultValue] << getTestIterationData()
    }

    @Unroll
    def "Reading a missing #propertyType property with a default gives the default"() {
        expect: "We read a property that doesn't exist with a default, we get the default"
        defaultValue == systemConfig."get${propertyType}Property"(MISSING_PROPERTY_KEY, defaultValue)

        where:
        [propertyType, _, _, _, defaultValue] << getTestIterationData()
    }

    @Unroll
    def "Reading a #propertyType property that isn't a #propertyType with a default throws an exception except for lists and strings"() {
        when: "We read a property that isn't of the right type with a default"
        systemConfig."get${propertyType}Property"(unconvertible, defaultValue)

        then: "A SystemConfigException is thrown"
        thrown SystemConfigException

        where:
        [propertyType, _, _, _, defaultValue, unconvertible] << getTestIterationData(String: true, List: true)
    }

    @Unroll
    def "Reading a #propertyType property will successfully assign and cast from int for numeric types"() {
        expect:
        INT_VALUE.asType(type) == systemConfig."get${propertyType}Property"(INT_PROPERTY_KEY)

        where:
        [propertyType, property, _, value, _, _, type] << getTestIterationData(String: true, Boolean: true, List: true)
    }

    @Unroll
    def "Reading a missing #propertyType property with a default value will successfully assign and cast from int for numeric types"() {
        expect:
        INT_DEFAULT_VALUE.asType(type) == systemConfig."get${propertyType}Property"(MISSING_PROPERTY_KEY, INT_DEFAULT_VALUE)

        where:
        [propertyType, _, _, _, _, _, type] << getTestIterationData(String: true, Boolean: true, List: true)
    }

    @Unroll
    def "Reading a property that is a float as #propertyType property fails type conversion"() {
        when: "It tries and fails to process"
        systemConfig."get${propertyType}Property"(FLOAT_PROPERTY_KEY)

        then:
        thrown SystemConfigException

        where:
        [propertyType] << getTestIterationData(String: true, Float: true, Double: true, Boolean: true, List: true)
    }

    def "Setting and resetting a property works"() {
        given: "No value for a given key"
        assert ! systemConfig.getMasterConfiguration().containsKey(MISSING_PROPERTY_KEY)

        when:
        systemConfig.setProperty(MISSING_PROPERTY_KEY, "a")

        then:
        systemConfig.getStringProperty(MISSING_PROPERTY_KEY) == "a"

        when:
        systemConfig.resetProperty(MISSING_PROPERTY_KEY, null)

        then:
        ! systemConfig.getMasterConfiguration().containsKey(MISSING_PROPERTY_KEY)
    }

    /**
     *  Get the data to use in where blocks as a list of lists.
     *
     *  The skip parameter allows to skip sets of properties
     *
     * @param skips  Map of type of skips. Set a type to true to skip that data
     *
     * @return the data as a list of lists
     */
    def getTestIterationData(Map<String, Boolean> skips = [:]) {

        Map<String, Boolean> doSkip = skips.withDefault { false }

        [
                //                 prop type, property,             propertyValue,          value,         defaultValue,          unconvertible,       real type
                doSkip.String ?:  ["String",  STRING_PROPERTY_KEY,  STRING_PROPERTY_VALUE,  STRING_VALUE,  STRING_DEFAULT_VALUE,  null,                String],
                doSkip.Int ?:     ["Int",     INT_PROPERTY_KEY,     INT_PROPERTY_VALUE,     INT_VALUE,     INT_DEFAULT_VALUE,     STRING_PROPERTY_KEY, Integer],
                doSkip.Long ?:    ["Long",    LONG_PROPERTY_KEY,    LONG_PROPERTY_VALUE,    LONG_VALUE,    LONG_DEFAULT_VALUE,    STRING_PROPERTY_KEY, Long],
                doSkip.Float ?:   ["Float",   FLOAT_PROPERTY_KEY,   FLOAT_PROPERTY_VALUE,   FLOAT_VALUE,   FLOAT_DEFAULT_VALUE,   STRING_PROPERTY_KEY, Float],
                doSkip.Double ?:  ["Double",  DOUBLE_PROPERTY_KEY,  DOUBLE_PROPERTY_VALUE,  DOUBLE_VALUE,  DOUBLE_DEFAULT_VALUE,  STRING_PROPERTY_KEY, Double],
                doSkip.Boolean ?: ["Boolean", BOOLEAN_PROPERTY_KEY, BOOLEAN_PROPERTY_VALUE, BOOLEAN_VALUE, BOOLEAN_DEFAULT_VALUE, STRING_PROPERTY_KEY, Boolean],
                doSkip.List ?:    ["List",    LIST_PROPERTY_KEY,    LIST_PROPERTY_VALUE,    LIST_VALUE,    LIST_DEFAULT_VALUE,    null,                List]
        ].findAll {
            // Only return data lists
            it instanceof List
        }
    }
}
