// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A log formatter that prints log in JSON format.
 */
public class JsonLogFormatter implements LogFormatter {
    private static final Logger LOG = LoggerFactory.getLogger(RequestLog.class);
    private final ObjectMapper objectMapper;

    /**
     *  Configure ObjectMapper.
     */
    public JsonLogFormatter() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule((new JodaModule()).addSerializer(Interval.class, new ToStringSerializer()));
        objectMapper.registerModule(new Jdk8Module().configureAbsentsAsNulls(false));
        objectMapper.registerModule(new AfterburnerModule());
    }

    @Override
    public String format(LogBlock logBlock) {
        try {
            return objectMapper.writeValueAsString(logBlock);
        } catch (JsonProcessingException jsonProcessingException) {
            String message = String.format("Exporting mega log line: '%s' to JSON failed.", logBlock);
            LOG.warn(message, jsonProcessingException);
            return logBlock.toString();
        }
    }
}
