// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response.Status;

/**
 * Response processor for finding missing partial data in Druid
 * <p>
 * In druid version 0.9.0 or later, druid implemented a feature that returns missing intervals for a given query.
 * For example
 *
 * <pre>
 * {@code
 * Content-Type: application/json
 * 200 OK
 * Date:  Mon, 10 Apr 2017 16:24:24 GMT
 * Content-Type:  application/json
 * X-Druid-Query-Id:  92c81bed-d9e6-4242-836b-0fcd1efdee9e
 * X-Druid-Response-Context: {
 *     "uncoveredIntervals": [
 *         "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z","2016-12-25T00:00:00.000Z/2017-
 *         01-03T00:00:00.000Z","2017-01-31T00:00:00.000Z/2017-02-01T00:00:00.000Z","2017-02-
 *         08T00:00:00.000Z/2017-02-09T00:00:00.000Z","2017-02-10T00:00:00.000Z/2017-02-
 *         13T00:00:00.000Z","2017-02-16T00:00:00.000Z/2017-02-20T00:00:00.000Z","2017-02-
 *         22T00:00:00.000Z/2017-02-25T00:00:00.000Z","2017-02-26T00:00:00.000Z/2017-03-
 *         01T00:00:00.000Z","2017-03-04T00:00:00.000Z/2017-03-05T00:00:00.000Z","2017-03-
 *         08T00:00:00.000Z/2017-03-09T00:00:00.000Z"
 *     ],
 *     "uncoveredIntervalsOverflowed": true
 * }
 * Content-Encoding:  gzip
 * Vary:  Accept-Encoding, User-Agent
 * Transfer-Encoding:  chunked
 * Server:  Jetty(9.2.5.v20141112)
 * }
 * </pre>
 *
 * The missing intervals are indicated in "uncoveredIntervals". We compare it to the missing intervals that we expects
 * from Partial Data V1. If "uncoveredIntervals" contains any interval that is not present in our expected
 * missing interval list, we can send back an error response indicating the mismatch in data availability before the
 * response is cached.
 */
public class DruidPartialDataResponseProcessor implements FullResponseProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(DruidPartialDataResponseProcessor.class);

    private final ResponseProcessor next;

    /**
     * Constructor.
     *
     * @param next  Next ResponseProcessor in the chain
     */
    public DruidPartialDataResponseProcessor(ResponseProcessor next) {
        this.next = next;
    }

    @Override
    public ResponseContext getResponseContext() {
        return next.getResponseContext();
    }

    @Override
    public FailureCallback getFailureCallback(DruidAggregationQuery<?> druidQuery) {
        return next.getFailureCallback(druidQuery);
    }

    @Override
    public HttpErrorCallback getErrorCallback(DruidAggregationQuery<?> druidQuery) {
        return next.getErrorCallback(druidQuery);
    }

    /**
     * If status code is 200, do the following
     *
     * <ol>
     *     <li>
     *         Extract uncoveredIntervalsOverflowed from X-Druid-Response-Context inside the JsonNode passed into
     *         DruidPartialDataResponseProcessor::processResponse, if it is true, invoke error response saying limit
     *         overflowed,
     *     </li>
     *     <li>
     *         Extract uncoveredIntervals from X-Druid-Response-Contex inside the JsonNode passed into
     *         DruidPartialDataResponseProcessor::processResponse,
     *     </li>
     *     <li>
     *         Parse both the uncoveredIntervals extracted above and allAvailableIntervals extracted from the union of
     *         all the query's datasource's availabilities from DataSourceMetadataService into SimplifiedIntervalLists,
     *     </li>
     *     <li>
     *         Compare both SimplifiedIntervalLists above, if allAvailableIntervals has any overlap with
     *         uncoveredIntervals, invoke error response indicating druid is missing some data that are we are expecting
     *         to exists.
     *     </li>
     * </ol>
     *
     * @param json  The json representing a druid data response
     * @param query  The query with the schema for processing this response
     * @param metadata  The LoggingContext to use
     */
    @Override
    public void processResponse(JsonNode json, DruidAggregationQuery<?> query, LoggingContext metadata) {
        validateJsonResponse(json, query);

        int statusCode = json.get(DruidJsonResponseContentKeys.STATUS_CODE.getName()).asInt();
        if (statusCode == Status.OK.getStatusCode()) {
            checkOverflow(json, query);

            SimplifiedIntervalList overlap = getOverlap(json, query);
            if (!overlap.isEmpty()) {
                logAndGetErrorCallback(ErrorMessageFormat.DATA_AVAILABILITY_MISMATCH.format(overlap), query);
            }
            if (next instanceof FullResponseProcessor) {
                next.processResponse(json, query, metadata);
            } else {
                next.processResponse(json.get(DruidJsonResponseContentKeys.RESPONSE.getName()), query, metadata);
            }
        } else if (statusCode == Status.NOT_MODIFIED.getStatusCode() && !(next instanceof FullResponseProcessor)) {
            logAndGetErrorCallback(
                    "Content Not Modified(304), but no etag cache response processor is available to process " +
                            "the 304 response",
                    query);
        } else {
            next.processResponse(json, query, metadata);
        }
    }

    /**
     * Validates JSON response object to make sure it contains all of the following information.
     * <ul>
     *     <li>X-Druid-Response-Context
     *         <ol>
     *             <li>uncoveredIntervals</li>
     *             <li>uncoveredIntervalsOverflowed</li>
     *         </ol>
     *     </li>
     *     <li>status-code</li>
     * </ul>
     *
     * @param json  The JSON response that is to be validated
     * @param query  The query with the schema for processing this response
     */
    private void validateJsonResponse(JsonNode json, DruidAggregationQuery<?> query) {
        if (json.getNodeType() == JsonNodeType.ARRAY) {
            logAndGetErrorCallback(ErrorMessageFormat.CONTEXT_AND_STATUS_MISSING_FROM_RESPONSE.format(), query);
        }

        if (!json.has(DruidJsonResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName())) {
            logAndGetErrorCallback(ErrorMessageFormat.DRUID_RESPONSE_CONTEXT_MISSING_FROM_RESPONSE.format(), query);
            return;
        }
        JsonNode druidResponseContext = json.get(DruidJsonResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName());
        if (!druidResponseContext.has(DruidJsonResponseContentKeys.UNCOVERED_INTERVALS.getName())) {
            logAndGetErrorCallback(
                    ErrorMessageFormat.UNCOVERED_INTERVALS_MISSING_FROM_RESPONSE.format(),
                    query
            );
            return;
        }
        if (!druidResponseContext.has(DruidJsonResponseContentKeys.UNCOVERED_INTERVALS_OVERFLOWED.getName())) {
            logAndGetErrorCallback(
                    ErrorMessageFormat.UNCOVERED_INTERVALS_OVERFLOWED_MISSING_FROM_RESPONSE.format(),
                    query
            );
            return;
        }
        if (!json.has(DruidJsonResponseContentKeys.STATUS_CODE.getName())) {
            logAndGetErrorCallback(ErrorMessageFormat.STATUS_CODE_MISSING_FROM_RESPONSE.format(), query);
        }
    }

    /**
     * Checks and invokes error if the number of missing intervals are overflowed, i.e. more than the configured limit.
     *
     * @param json  The json object containing the overflow flag
     * @param query  The query with the schema for processing this response
     */
    private void checkOverflow(JsonNode json, DruidAggregationQuery<?> query) {
        if (json.get(DruidJsonResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName())
                .get(DruidJsonResponseContentKeys.UNCOVERED_INTERVALS_OVERFLOWED.getName())
                .asBoolean()
        ) {
            logAndGetErrorCallback(
                    ErrorMessageFormat.TOO_MANY_INTERVALS_MISSING.format(
                            query.getContext().getUncoveredIntervalsLimit()
                    ),
                    query
            );
        }
    }

    /**
     * Logs and gets error call back on the response with the provided error message.
     *
     * @param message  The error message passed to the logger and the exception
     * @param query  The query with the schema for processing this response
     */
    private void logAndGetErrorCallback(String message, DruidAggregationQuery<?> query) {
        LOG.error(message);
        getErrorCallback(query).dispatch(
                Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                "The server encountered an unexpected condition which prevented it from fulfilling the request.",
                message);
    }

    /**
     * Returns the overlap between uncoveredIntervals from Druid and missing intervals that Fili expects.
     *
     * @param json  The JSON node that contains the uncoveredIntervals from Druid, for example
     * <pre>
     * {@code
     * X-Druid-Response-Context: {
     *     "uncoveredIntervals": [
     *         "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z","2016-12-25T00:00:00.000Z/2017-
     *         01-03T00:00:00.000Z","2017-01-31T00:00:00.000Z/2017-02-01T00:00:00.000Z","2017-02-
     *         08T00:00:00.000Z/2017-02-09T00:00:00.000Z","2017-02-10T00:00:00.000Z/2017-02-
     *         13T00:00:00.000Z","2017-02-16T00:00:00.000Z/2017-02-20T00:00:00.000Z","2017-02-
     *         22T00:00:00.000Z/2017-02-25T00:00:00.000Z","2017-02-26T00:00:00.000Z/2017-03-
     *         01T00:00:00.000Z","2017-03-04T00:00:00.000Z/2017-03-05T00:00:00.000Z","2017-03-
     *         08T00:00:00.000Z/2017-03-09T00:00:00.000Z"
     *     ],
     *     "uncoveredIntervalsOverflowed": true
     * }
     * }
     * </pre>
     * @param query  The Druid query that contains the missing intervals that Fili expects
     *
     * @return the overlap between uncoveredIntervals from Druid and missing intervals that Fili expects.
     */
    private SimplifiedIntervalList getOverlap(JsonNode json, DruidAggregationQuery<?> query) {
        List<Interval> intervals = new ArrayList<>();
        for (JsonNode jsonNode :
                json.get(DruidJsonResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName())
                        .get(DruidJsonResponseContentKeys.UNCOVERED_INTERVALS.getName())
                ) {
            intervals.add(new Interval(jsonNode.asText()));
        }
        SimplifiedIntervalList druidIntervals = new SimplifiedIntervalList(intervals);

        return druidIntervals.intersect(
                query.getDataSource().getPhysicalTable().getAvailableIntervals()
        );
    }
}
