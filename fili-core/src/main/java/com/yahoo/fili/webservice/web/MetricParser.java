// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.web;

import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * This class will separate the metrics and associated filters from metricString.
 * It will then create an array of JSON objects which contains metric name and metric filter as attributes
 */
public class MetricParser {
    private static final Logger LOG = LoggerFactory.getLogger(MetricParser.class);

    /**
     * This function converts the string of metrics extracted from the url into JSONArray.
     *
     * @param metricString  An Api metric string eg:
     * <pre>{@code metricString = metric1(AND(dim1|id-in[a,b],dim2|id-in[c,d])),metric2 }</pre>
     *
     * @return A JSONArray constructed from a given metricString. Eg:
     * <pre>{@code {[{"filter":{"AND":{"dim2|id-in":["abc","xyz"],"dim3|id-in":["mobile","tablet"]}}, "name":"metric1"},
     * {"filter":{},"name":"metric2"}]}}</pre>

     * @throws JSONException if a JSON cannot be constructed successfully from the given metricString
     * @throws IllegalArgumentException if metricString is empty or the metricString has unbalanced brackets
     */
    public static JSONArray generateMetricFilterJsonArray(String metricString) throws JSONException {
        if (metricString.length() == 0 || !(isBracketsBalanced(metricString))) {
            LOG.error("Metrics parameter values are invalid. The string is: " + metricString);
            throw new IllegalArgumentException("Metrics parameter values are invalid. The string is: " + metricString);
        }
        Map<String, String> mapper = new HashMap<>();

        //modifiedString contains only metric name and '-' separated filter encoded key. Since there is no brackets
        //and filter details, it easy to split by comma. ex: modifiedString=metric1-123,metric2-234
        String[] metrics = encodeMetricFilters(metricString, mapper).split(",");

        //looping each metric in a metrics array
        List<String> metricList = new ArrayList<>();
        for (String metric : metrics) {
            //Separating metric and '-' separated filter encode metricFilterArray[0] will be metric name
            //and metricFilterArray[1] will be key of the mapper contains respective filter string of the metric
            String[] metricFilterArray = metric.split("-");
            //Retrieving the filter string from the mapper and creating a metric set with metric as key
            //and filter as value
            String metricFilter = metricFilterArray.length > 1 ? mapper.get("-" + metricFilterArray[1]) : "";
            metricList.add(getJsonString(metricFilterArray[0], metricFilter));
        }
        StringBuilder finalJson = new StringBuilder();
        //preparing complete json string by appending brackets to metric name and metric filter
        for (String jsonToken : metricList) {
            finalJson.append("{" + jsonToken + "},");
        }
        return new JSONArray("[" + finalJson.toString().substring(0, finalJson.toString().length() - 1) + "]");
    }

    /**
     * Returns a String that is formatted to a JSON string.
     *
     * @param metricName  the name of the metric
     * @param metricFilter  the filter associated with the metric. Could be empty or have a value
     *
     * @return A String in JSON format for a given metric and filter.
     * For eg: "filter":{"AND":"property|id-in:[14,125],country|id-in:[US,IN]},"name":"foo"
     */
    private static String getJsonString(String metricName, String metricFilter) {
        if (metricFilter.isEmpty()) {
            metricFilter = "\"filter\": {}";
        } else {
            metricFilter = "\"filter\":{\"AND\":\"" +
                    metricFilter.replace("AND", "").replace("(", "").replace(")", "") + "\" }";

        }
        return "\"name\": \"" + metricName + "\"," + metricFilter;
    }

    /**
     * Returns modified metrics string. Aggregated metric filter details are stripped and stored in mapper.
     * Metric filter is replaced by hashkey which can be used to retrieve the filter from the mapper
     *
     * @param metricString  The metricString from the API request
     * @param mapper  An empty HashMap.  It will have a randomNumber as key and filter for a metric as value
     *
     * @return filter and metric separated string For eg: metric1-1234,metric2-2341
     */
    private static String encodeMetricFilters(String metricString, Map<String, String> mapper) {
        String finalString = metricString;
        int counter = 0;
        int startIndex = 0;
        int endIndex = 0;
        for (int i = 0; i < metricString.length(); i++) {
            char currentChar = metricString.charAt(i);
            startIndex = (currentChar == '(' && counter == 0) ? i : startIndex;
            counter = (currentChar == '(') ? counter + 1 : counter;
            if (currentChar == ')') {
                endIndex = i;
                counter = counter - 1;
            }
            if (counter == 0 && startIndex != 0) {
                String filterString = metricString.substring(startIndex, endIndex + 1);
                finalString = finalString.replace(filterString, "-" + startIndex);
                mapper.put("-" + startIndex, filterString);
                startIndex = 0;
            }
        }
        return finalString;
    }

    /**
     * Check if the brackets are balanced.
     *
     * @param metricString  The metricString from the API request
     *
     * @return True if the given metricString has balanced brackets and false if the brackets are not balanced
     */
    private static boolean isBracketsBalanced(String metricString) {
        // TODO: Move this to a constant map
        Map<Character, Character> brackets = new HashMap<>();
        brackets.put('[', ']');
        brackets.put('(', ')');

        // Use a stack to push in opening brackets and pop closing brackets as we find them
        Stack<Character> stack = new Stack<>();
        for (int i = 0; i < metricString.length(); i++) {
            char currentChar = metricString.charAt(i);
            if (brackets.containsKey(currentChar)) {
                // Push new opening bracket
                stack.push(currentChar);
            } else if (brackets.values().contains(currentChar) && (currentChar != brackets.get(stack.pop()))) {
                // Didn't find a matching opening bracket on the top of the stack for the closing bracket
                return false;
            }
        }

        // All matched if there's no un-matched opening brackets
        return stack.empty();
    }
}
