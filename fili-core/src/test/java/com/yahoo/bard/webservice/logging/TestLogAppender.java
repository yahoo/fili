// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Allows testing of logging output. Attaches as a Log appender and collects log messages for later retrieval by test.
 *
 * @see com.yahoo.bard.webservice.web.filters.LogFilterSpec
 */
public class TestLogAppender extends AppenderBase<ILoggingEvent> implements Closeable {

    public static final int MAX_MESSAGE_WAIT_MS = 10000;
    private final Logger logger;
    private final List<ILoggingEvent> events = Collections.synchronizedList(new ArrayList<>());

    /**
     * Find root of all loggers and add this appender.
     */
    public TestLogAppender() {
        // Get the root logger
        logger = (Logger) LoggerFactory.getILoggerFactory().getLogger(ROOT_LOGGER_NAME);

        // Add this appender to the root logger
        logger.addAppender(this);

        // Set my context to it's context
        setContext(logger.getLoggerContext());

        // Start the appender
        start();
    }


    @Override
    protected void append(ILoggingEvent logEvent) {
        events.add(logEvent);
    }

    /**
     * Clears collected log messages.
     */
    public void clear() {
        events.clear();
    }

    @Override
    public void stop() {
        logger.detachAppender(this);
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Retrieve all LogEvent collected.
     *
     * @return LogEvents
     */
    public List<ILoggingEvent> getEvents() {
        return Collections.unmodifiableList(events);
    }

    /**
     * Retrieve a log message.
     *
     * @param index  which message. 0-based
     *
     * @return Message string
     */
    public String getMessage(int index) {
        if (size() < index) {
            try {
                events.wait(MAX_MESSAGE_WAIT_MS);
            } catch (InterruptedException ignored) {
                // Empty
            }
        }
        return events.get(index).getFormattedMessage();
    }

    /**
     * Retrieve all log messages.
     *
     * @return the list of messages
     */
    public List<String> getMessages() {
        return getEvents().stream()
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());
    }

    /**
     * Retrieve number of available events.
     *
     * @return the number of events
     */
    public int size() {
        return events.size();
    }

    /**
     * Retrieve a log message split into lines at any \t character.
     *
     * @param index  which message 0-based
     *
     * @return Message string with lines split at \t
     */
    public List<String> getMessageAsTabSplitLines(int index) {
        return Arrays.asList(getMessage(index).split("\t"));
    }

    /**
     * Retrieve a log message split into lines by regex.
     *
     * @param index  which message. 0-based
     * @param regex  the delimiting regular expression
     *
     * @return Message string with lines split at regex
     */
    public List<String> getMessageAsRegexSplitLines(int index, String regex) {
        return Arrays.asList(getMessage(index).split(regex));
    }
}
