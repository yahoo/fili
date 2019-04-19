// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;

/**
 * Holder for the system-wide date-time formatters.
 */
public class DateTimeFormatterFactory {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String OUTPUT_DATETIME_FORMAT = SYSTEM_CONFIG.getPackageVariableName("output_datetime_format");
    public static DateTimeFormatter DATETIME_OUTPUT_FORMATTER;

    // Default JodaTime zone to UTC
    private static final DateTimeZone SYSTEM_TIME_ZONE = DateTimeZone.forID(SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("timezone"),
            "UTC"
    ));

    /**
     * Get the output formatter for the system.
     *
     * @return the output formatter, pulling it from a configuration if we've not gotten it before.
     */
    public static DateTimeFormatter getOutputFormatter() {
        if (DATETIME_OUTPUT_FORMATTER == null) {
            String formatString = SYSTEM_CONFIG.getStringProperty(OUTPUT_DATETIME_FORMAT, "yyyy-MM-dd' 'HH:mm:ss.SSS");
            DATETIME_OUTPUT_FORMATTER = DateTimeFormat.forPattern(formatString);
        }
        return DATETIME_OUTPUT_FORMATTER;
    }

    final public static DateTimeFormatter FULLY_OPTIONAL_DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(
                    (DateTimePrinter) null,
                    new DateTimeParser[] {
                            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").getParser(),
                            DateTimeFormat.forPattern("yyyy-MM-dd' 'HH:mm:ss.SSS").getParser(),
                            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").getParser(),
                            DateTimeFormat.forPattern("yyyy-MM-dd' 'HH:mm:ss").getParser(),
                            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm").getParser(),
                            DateTimeFormat.forPattern("yyyy-MM-dd' 'HH:mm").getParser(),
                            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH").getParser(),
                            DateTimeFormat.forPattern("yyyy-MM-dd' 'HH").getParser(),
                            DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
                            DateTimeFormat.forPattern("yyyy-MM").getParser(),
                            DateTimeFormat.forPattern("yyyy").getParser()
                    }
            ).toFormatter().withZone(SYSTEM_TIME_ZONE);
}
