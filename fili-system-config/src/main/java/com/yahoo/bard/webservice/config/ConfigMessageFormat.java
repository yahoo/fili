// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

import com.yahoo.bard.webservice.MessageFormatter;

/**
 * Formatting messages for the Configuration System.
 */
public enum ConfigMessageFormat implements MessageFormatter {

    // CHECKSTYLE:OFF
    MODULE_IO_EXCEPTION (
            "IoException while loading modules: %s"
    ),
    CIRCULAR_DEPENDENCY(
            "A circular dependency exists in the module configurations module: %s is in dependency chain: %s."
    ),
    INVALID_MODULE_CONFIGURATION(
            "Invalid module configuration: %s"
    ),
    INVALID_MODULE_NAME(
            "Invalid module name: '%s', reason: %s"
    ),

    TOO_MANY_USER_CONFIGS(
            "Only zero or one user configurations is allowed, found %d",
            "Only zero or one user configurations is allowed, found resources: %s"
    ),

    INVALID_TEST_APPLICATION_CONFIG(
            "Test application config from jar found at classpath location '%s'.  Only non jar test application configurations are allowed."
    ),

    TOO_MANY_APPLICATION_CONFIGS(
            "Only zero or one application configurations is allowed, found %d",
            "Only zero or one application configurations is allowed, found resources: %s"
    ),

    CONFIGURATION_LOAD_ERROR(
            "Error while loading resource with URI: '%s' as a properties file."
    ),

    MODULE_NAME_DUPLICATION(
            "Class path resource '%s' contains a duplicate module name '%s'."
    ),

    MODULE_NAME_MISSING(
            "No module name in resource '%s'"
    ),

    NO_SUCH_MODULE(
            "Module '%s' does not exist."
    ),

    MISSING_DEPENDENCY(
            "Module '%s' referenced in path '%s' is not defined."
    ),
    // These are not an error message, only used for logging
    MODULE_FOUND_MESSAGE(
            "Module '%s' found in resource '%s'"
    ),
    MODULE_DEPENDS_ON_MESSAGE(
            "Module '%s' depends on modules '%s'"
    ),

    RESOURCE_LOAD_MESSAGE(
            "While loading resources named '%s', found '%s'"
    )

    ;

    // CHECKSTYLE:ON

    private final String messageFormat;
    private final String loggingFormat;

    /**
     * An error message formatter with the same message for logging and messaging.
     *
     * @param messageFormat The format string for logging and messaging
     */
    ConfigMessageFormat(String messageFormat) {
        this(messageFormat, messageFormat);
    }

    /**
     * An error message formatter with different messages for logging and messaging.
     *
     * @param messageFormat The format string for messaging
     * @param loggingFormat The format string for logging
     */
    ConfigMessageFormat(String messageFormat, String loggingFormat) {
        this.messageFormat = messageFormat;
        this.loggingFormat = loggingFormat;
    }

    @Override
    public String getMessageFormat() {
        return messageFormat;
    }

    @Override
    public String getLoggingFormat() {
        return loggingFormat;
    }
}
