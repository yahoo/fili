// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to provide a log formatter instance.
 */
public class LogFormatterProvider {

    private static final Logger LOG = LoggerFactory.getLogger(LogFormatterProvider.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final String DEFAULT_LOG_FORMATTER_IMPL = JsonLogFormatter.class.getCanonicalName();

    private static final String LOG_FORMATTER_IMPLEMENTATION_KEY = SystemConfigProvider.getInstance().getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("log_formatter_implementation"),
            DEFAULT_LOG_FORMATTER_IMPL
    );

    /**
     * The instance of the Log Formatter.
     */
    private static final LogFormatter LOG_FORMATTER = getInstance();

    /**
     *  Get an instance of LogFormatter.
     *
     *  @return an instance of LogFormatter
     */
    public static LogFormatter getInstance() {
        if (LOG_FORMATTER == null) {
            try {
                String logFormatterImplementation = SystemConfigProvider.getInstance().getStringProperty(
                        SYSTEM_CONFIG.getPackageVariableName(LOG_FORMATTER_IMPLEMENTATION_KEY),
                        DEFAULT_LOG_FORMATTER_IMPL
                );

                return (LogFormatter) Class.forName(logFormatterImplementation).newInstance();
            } catch (Exception exception) {
                LOG.error("Exception while loading Log formatter: {}", exception);
                throw new IllegalStateException(exception);
            }
        }

        return LOG_FORMATTER;
    }
}
