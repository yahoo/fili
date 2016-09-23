// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.logging.TestLogAppender

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

/**
 * Contains utility methods to aid in testing logging.
 */
class LoggingTestUtils {

    /**
     * Given a TestLogAppender, finds the line containing the {@code uuid}, and returns the values of the
     * {@code method}, {@code status}, {@code code}, and {@code logMessage} fields.
     *
     * We assume that the line containing the "uuid" field is well-formed JSON. If there is no log line that contains
     * the "uuid", then we return an empty Map.
     *
     * @param logAppender  The log that contains the lines of interest
     * @param mapper  The ObjectMapper used to parse the uuid line into a JsonNode for extraction
     *
     * @return A map mapping the keys "method", "status", "code", and "logMessage" to their appropriate values
     */
    static Map<String, String> extractResultsFromLogs(TestLogAppender logAppender, ObjectMapper mapper) {
        JsonNode json = mapper.readValue(logAppender.getMessages().find { it.contains(/"uuid"/) }, JsonNode.class)
        //Need to explicitly check for null, because JsonNode overrides `asBoolean` to `false`, so
        //applying Groovy truth to a JsonNode will always return false, unless the `JsonNode` happens to contain a
        //boolean.
        return json == null ?
                [:] :
                ["method", "status", "code", "logMessage"].collectEntries { [(it): json.findValue(it).toString()] }
    }
}
