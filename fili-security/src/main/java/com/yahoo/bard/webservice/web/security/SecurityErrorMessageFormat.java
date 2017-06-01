// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security;

import com.yahoo.bard.webservice.MessageFormatter;

/**
 * Message formats for security module errors.
 */
public enum SecurityErrorMessageFormat implements MessageFormatter {

    DIMENSION_MISSING_MANDATORY_ROLE(
            "A required role is missing for dimension '%s'.  Contact your security administrator for assistance.",
            "User %s does not have a mandatory role on dimension '%s'. Access denied."
    );

    private final String messageFormat;
    private final String loggingFormat;

    /**
     * An error message formatter with the same message for logging and messaging.
     *
     * @param messageFormat The format string for logging and messaging
     */
    SecurityErrorMessageFormat(String messageFormat) {
        this(messageFormat, messageFormat);
    }

    /**
     * An error message formatter with different messages for logging and messaging.
     *
     * @param messageFormat The format string for messaging
     * @param loggingFormat The format string for logging
     */
    SecurityErrorMessageFormat(String messageFormat, String loggingFormat) {
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
