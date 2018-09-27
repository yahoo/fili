// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import static com.yahoo.bard.webservice.util.JsonSortStrategy.SORT_MAPS

import com.yahoo.bard.webservice.application.ObjectMappersSuite

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import groovy.json.JsonException
import groovy.util.logging.Slf4j
import spock.lang.Specification

/**
 * Abstract to not run as a test suite, but sub-classes Specification so power assert works correctly
 */
@Slf4j(value = "LOG")
abstract class GroovyTestUtils extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    private static final JsonSlurper JSON_PARSER = new JsonSlurper()

    /**
     * Use Spock power assert to compare JSON string values
     *
     * @param actual actual JSON object
     * @param expected expected JSON object
     * @param sortStrategy Strategy to use for sorting the JSON when parsing. Defaults to SORT_MAPS, which complies with
     * the JSON specification's requirement that objects are unordered and arrays are ordered
     *
     * @see <a href="http://json.org/">JSON.org</a>
     */
    static boolean compareJson(String actual, String expected, JsonSortStrategy sortStrategy = SORT_MAPS) {
        if (actual.equals(expected)) {
            // Objects match, all is well
            return true
        }

        try {
            // Parse the JSON strings into objects
            //Use two separate slurpers because slurper optimizations mean that the two parsed maps will share
            //maps that are identical.
            JsonSlurper jsonSlurper = new JsonSlurper(sortStrategy)

            JsonNode actualRoot = MAPPER.readTree(actual);
            JsonNode expectedRoot = MAPPER.readTree(expected);

            Utils.canonicalize(actualRoot, MAPPER, false);
            Utils.canonicalize(expectedRoot, MAPPER, false);

            def actualProcessed = MAPPER.writer().withDefaultPrettyPrinter().writeValueAsString(actualRoot);
            def expectedProcessed = MAPPER.writer().withDefaultPrettyPrinter().writeValueAsString(expectedRoot);

            def actualJson = jsonSlurper.parseText(actualProcessed)
            def expectedJson = jsonSlurper.parseText(expectedProcessed)

            def actualPagination = actualJson.get("meta")?.get("pagination")
            def expectedPagination = expectedJson.get("meta")?.get("pagination")

            String actualFirst = actualPagination?.remove("first")
            String actualLast = actualPagination?.remove("last")
            String actualNext = actualPagination?.remove("next")
            String actualPrevious = actualPagination?.remove("previous")

            String expectedFirst = expectedPagination?.remove("first")
            String expectedLast = expectedPagination?.remove("last")
            String expectedNext = expectedPagination?.remove("next")
            String expectedPrevious = expectedPagination?.remove("previous")


            boolean firstMatch = expectedFirst && actualFirst ?
                    compareURL(expectedFirst, actualFirst) :
                    //If one of them is null, they should both be null.
                    expectedFirst == actualFirst

            boolean lastMatch = expectedLast && actualLast ?
                    compareURL(expectedLast, actualLast) :
                    expectedLast == actualLast

            boolean nextMatch = expectedNext && actualNext ?
                    compareURL(expectedNext, actualNext) :
                    expectedNext == actualNext

            boolean previousMatch = expectedPrevious && actualPrevious ?
                    compareURL(expectedPrevious, actualPrevious) :
                    expectedPrevious == actualPrevious

            // Compare the objects
            if ([actualJson.equals(expectedJson), nextMatch, previousMatch, firstMatch, lastMatch].every()) {
                // Objects match, all is well
                return true
            }

            if (actualNext) {
                actualPagination.put("next", actualNext)
            }
            if (actualPrevious) {
                actualPagination.put("previous", actualPrevious)
            }
            if (actualFirst) {
                actualPagination.put("first", actualFirst)
            }
            if (actualLast) {
                actualPagination.put("last", actualLast)
            }
            if (expectedNext) {
                expectedPagination.put("next", expectedNext)
            }
            if (expectedPrevious) {
                expectedPagination.put("previous", expectedPrevious)
            }
            if (expectedFirst) {
                expectedPagination.put("first", expectedFirst)
            }
            if (expectedLast) {
                expectedPagination.put("last", expectedLast)
            }

            // Convert the objects back into Strings to get comparisons, since the objects don't match
            String actualText = MAPPER.writeValueAsString(actualJson)
            String expectedText = MAPPER.writeValueAsString(expectedJson)

            // Use power assert against strings to get comparisons
            assert actualText == expectedText
        } catch (JsonProcessingException | JsonException e) {
            LOG.warn("Unable to parse JSON", e)

            // Compare the inputs to get a sense of what went wrong
            assert actual == expected

            // Throw the exception because we shouldn't be comparing JSON unless we expect JSON
            throw e
        }
    }

    /**
     * Use Spock power assert to compare the toString values
     *
     * @param actual actual object
     * @param expected expected object
     */
    static boolean compareObjects(Object actual, Object expected) {
        // Compare the objects
        if (actual.equals(expected)) {
            // Objects match, all is well
            return true
        }

        // Convert the objects back into Strings to get comparisons, since the objects don't match
        String actualText = MAPPER.writeValueAsString(actual)
        String expectedText = MAPPER.writeValueAsString(expected)

        // Use power assert against strings to get comparisons
        assert actualText == expectedText
        return false
    }

    /**
     * Compare URL regardless of parameter order
     * @param actual actual object
     * @param expected expected object
     */
    static boolean compareURL(String actual, String expected) {
        if (actual.equals(expected)) {
            // Objects match, all is well
            return true
        }

        // Accept any order of parameters
        TreeSet actualList = actual.split("[?&>]") as TreeSet
        TreeSet expectedList = expected.split("[?&>]") as TreeSet
        assert actualList == expectedList
        return actualList == expectedList
    }

    /**
     * Splits a String representation of header links to the URL and rel.
     * <p>
     * Given a String representing a list of links of the form [URL1 ; rel="name1", URL ; rel="name2", ...] returns a
     * mapping from rel names to URLS of the form
     * <p>
     * [
     * "name1" : URL1
     * "name2" : URL2
     * ...
     * ]
     * <p>
     * Assumes there are no commas in the URLs or link names.
     *
     * @param headerLinks  The header links to be split
     *
     * @return A mapping from rel names to the assoicated URL.
     */
    static Map<String, String> splitHeaderLinks(String headerLinks) {
        headerLinks.tokenize(',') //Split into links
                .collect {it.split("; rel=")} //Split into URL/rel pairs
                .collectEntries { [(it[1].trim()): it[0].trim()] } //Trim the pairs and put them into a rel/url map
    }

    /**
     * A special-purpose operation that verifies that an error response is correct.
     * <p>
     * This function expects JSON Strings. It filters out the context from the Druid query contained in the error
     * message (because it contains an untestable UUID for the query) and it verifies that the queryId is present, but
     * does not attempt to perform any comparisions against it, since that is also an untestable UUID.
     *
     *
     * @param errorBody  The error generated by the test
     * @param expectedBody  The expected error body
     *
     * @return True if the two error responses are equal on the fields that matter (i.e. not UUID's), false otherwise
     */
    static boolean compareErrorPayload(String errorBody, String expectedBody) {
        Map errorBodyJson = JSON_PARSER.parseText(errorBody)
        Map expectedBodyJson = JSON_PARSER.parseText(expectedBody)

        // Using for loops for better error messages.
        for (item in errorBodyJson) {
            def value = item.key == "druidQuery" ? stripQueryId(item.value as Map) : item.value
            assert value == expectedBodyJson[item.key] || (item.key == "requestId" && item.value)
        }
        for (item in expectedBodyJson) {
            assert item.key in errorBodyJson.keySet()
        }
        return true
    }

    /**
     * Given a map representing a (possibly nested) Druid query, strips the (random, unique, and untestable) queryId
     * out of the query's context, and out of the context of every nested query.
     *
     * @param map  The druid query with query id's that need to be stripped
     *
     * @return  A copy of the math with all query ids stripped.
     */
    static Map stripQueryId(Map query) {
        return query?.collectEntries {
            if (it.key == "context") {
                [(it.key): it.value.findAll { entry -> entry.key != "queryId"}]
            } else if (it.key == "dataSource" && it.value.type == "query") {
                Map newDataSource = new LinkedHashMap(it.value as Map)
                newDataSource.query = stripQueryId(newDataSource.query as Map)
                [(it.key): newDataSource]
            } else {
                [(it.key): it.value]
            }
        }
    }
}
