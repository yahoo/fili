// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;

/**
 * Message Formatter interface provides shared functionality to support classes which provide formatted logging and
 * messaging.
 */
public interface MessageFormatter {

    /**
     * The message format used for publishing out of the system, typically in error messages.
     *
     * @return The format for messages
     */
    String getMessageFormat();

    /**
     * The message format used for logging.
     *
     * @return The format for log messages
     */
    String getLoggingFormat();

    /**
     * Format a message for reporting to a user/client.
     *
     * @param values The values to populate the format string
     *
     * @return The user message
     */
    default String format(Object... values) {
        return String.format(getMessageFormat(), values);
    }

    /**
     * Format a message for writing to the log.
     *
     * @param values The values to populate the format string
     *
     * @return The logging message
     */
    default String logFormat(Object... values) {
        return String.format(getLoggingFormat(), values);
    }
}
