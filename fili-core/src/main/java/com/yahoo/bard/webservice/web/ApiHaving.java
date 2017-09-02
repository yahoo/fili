// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.HAVING_ERROR;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.HAVING_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.HAVING_METRIC_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.HAVING_NON_NUMERIC;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.HAVING_OPERATOR_INVALID;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.util.FilterTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Collections;
import java.util.Objects;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * The Bard-level representation of a Druid Having clause. This class assumes that all metrics in the associated
 * having clause are defined.
 */
public class ApiHaving {
    private static final Logger LOG = LoggerFactory.getLogger(ApiHaving.class);

    private final LogicalMetric metric;
    private final HavingOperation operation;
    private final List<Double> values;

    /*  url having query pattern:  (metric name)-(operation)[(value or comma separated numeric values)]?
     *
     *  e.g.    revenue-lt[245]
     *          clicks-gt[7]
     *
     *          metric name:    revenue     clicks
     *          operation:      lessThan    greaterThan
     *          values:         245         7
     */
    private static final Pattern QUERY_PATTERN = Pattern.compile("([^\\|]+)-([^\\[]+)\\[([^\\]]+)\\]?");

    /**
     * Parses the URL having Query and generates the ApiHaving object.
     *
     * @param havingQuery  Expects a URL having query String in the format:
     * <p>
     * <code>(metric name)-(operation)[?(value or comma separated numeric values)]?</code>
     * @param metricDictionary  cache containing all the valid metric objects.
     *
     * @throws BadHavingException  when having pattern is not matched or when any of its properties are not valid.
     */
    public ApiHaving(@NotNull String havingQuery, MetricDictionary metricDictionary) throws BadHavingException {
        LOG.trace("Having query: {} MetricDictionary: {}", havingQuery, metricDictionary);

        Matcher tokenizedQuery = QUERY_PATTERN.matcher(havingQuery);

        if (!tokenizedQuery.matches()) {
            LOG.debug(HAVING_INVALID.logFormat(havingQuery));
            throw new BadHavingException(HAVING_INVALID.format(havingQuery));
        }

        metric = extractMetric(tokenizedQuery, metricDictionary);

        operation = extractOperation(tokenizedQuery);

        values = extractValues(tokenizedQuery, havingQuery);
    }

    /**
     * Constructor for an ApiHaving object whose data has already been parsed.
     *
     * @param metric  The metric to perform the "having" check on.
     * @param operation  The operation to perform (i.e. greater than, less than).
     * @param values  The numbers to compare the metric to.
     */
    public ApiHaving(LogicalMetric metric, HavingOperation operation, List<Double> values) {
        this.metric = metric;
        this.operation = operation;
        this.values = Collections.unmodifiableList(values);
    }

    // CHECKSTYLE:OFF
    public ApiHaving withLogicalMetric(@NotNull LogicalMetric metric) {
        return new ApiHaving(metric, operation, values);
    }

    public ApiHaving withOperation(@NotNull HavingOperation operation) {
        return new ApiHaving(metric, operation, values);
    }

    public ApiHaving withValues(@NotNull List<Double> values) {
        return new ApiHaving(metric, operation, values);
    }
    // CHECKSTYLE:ON

    public LogicalMetric getMetric() {
        return metric;
    }

    public HavingOperation getOperation() {
        return operation;
    }

    public List<Double> getValues() {
        return values;
    }

    /**
     * Extracts the metric to be examined from the having tokenizedQuery.
     *
     * @param tokenizedQuery  The parsed "having" tokenizedQuery.
     * @param metricDictionary  The cache containing all the valid metric objects.
     *
     * @return The metric to be examined.
     * @throws BadHavingException If the metric does not exist.
     */
    private LogicalMetric extractMetric(
            Matcher tokenizedQuery,
            MetricDictionary metricDictionary
    ) throws BadHavingException {
        String metricName = tokenizedQuery.group(1);
        LogicalMetric extractedMetric = metricDictionary.get(metricName);
        if (extractedMetric == null) {
            LOG.debug(HAVING_METRIC_UNDEFINED.logFormat(metricName));
            throw new BadHavingException(HAVING_METRIC_UNDEFINED.logFormat(metricName));
        }
        return extractedMetric;
    }

    /**
     * Extracts the operation to be performed by the having query.
     *
     * @param query  The parsed having query
     *
     * @return The operation to be performed by the having query.
     * @throws BadHavingException if the operation name in the query is malformed.
     */
    private HavingOperation extractOperation(Matcher query) throws BadHavingException {
        String operationName = query.group(2);
        try {
            return HavingOperation.fromString(operationName);
        } catch (IllegalArgumentException ignored) {
            LOG.debug(HAVING_OPERATOR_INVALID.logFormat(operationName));
            throw new BadHavingException(HAVING_OPERATOR_INVALID.format(operationName));
        }
    }

    /**
     * Extracts the values to be used in the having query from the query.
     *
     * @param query  The tokenized having query
     * @param havingQuery  The raw query. Used for logging.
     *
     * @return The set of values to be used in the having query.
     * @throws BadHavingException If the fragment of the query that specifies the values is malformed, or at least one
     * of the values is not a number.
     */
    private List<Double> extractValues(Matcher query, String havingQuery) throws BadHavingException {
        List<String> stringValues = createValueList(query, havingQuery);

        //Allows us to parse the values in a streamy way without losing information about the first value that
        //fails to parse.
        Function<String, Double> toNumber = value -> {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException ignored) {
                //Allows us to extract the value outside the stream.
                throw new NumberFormatException(value);
            }
        };

        try {
            return stringValues.stream().map(toNumber).collect(Collectors.toCollection(LinkedList::new));
        } catch (NumberFormatException e) {
            LOG.debug(HAVING_NON_NUMERIC.format(e.getMessage()));
            throw new BadHavingException(HAVING_NON_NUMERIC.format(e.getMessage()));
        }
    }

    /**
     * Given a string representing a set of values to be used in the having query, turns the string into a set.
     *
     * @param query  The tokenized having query.
     * @param havingQuery The raw query. Used for logging.
     *
     * @return A set of strings representing the values to be used in the having query.
     * @throws BadHavingException If the string of values is malformed.
     */
    private List<String> createValueList(Matcher query, String havingQuery) throws BadHavingException {
        try {
            // replaceAll takes care of any leading ['s or trailing ]'s which might mess up the values set.
            return new LinkedList<>(
                    FilterTokenizer.split(
                            query.group(3)
                                    .replaceAll("\\[", "")
                                    .replaceAll("\\]", "")
                                    .trim()
                    )
            );
        } catch (IllegalArgumentException e) {
            LOG.debug(HAVING_ERROR.logFormat(havingQuery, e.getMessage()), e);
            throw new BadHavingException(HAVING_ERROR.format(havingQuery, e.getMessage()), e);
        }
    }

    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof ApiHaving)) { return false; }

        ApiHaving apiHaving = (ApiHaving) o;

        return
                Objects.equals(metric, apiHaving.metric) &&
                Objects.equals(operation, apiHaving.operation) &&
                Objects.equals(values, apiHaving.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metric, operation, values);
    }
}
