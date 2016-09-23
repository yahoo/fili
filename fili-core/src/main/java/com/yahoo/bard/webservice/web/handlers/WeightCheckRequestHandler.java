// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.WeightCheckResponseProcessor;
import com.yahoo.bard.webservice.web.util.QueryWeightUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

/**
 * Weight check request handler determines whether a request should be processed based on estimated query cost.
 * <ul>
 *     <li>If the dimensions of the query are sufficiently low cardinality, the request is allowed.
 *     <li>Otherwise, send a simplified version of the query to druid asynchronously to measure the cardinality of the
 * results.
 *     <li>If the cost is too high, return an error, otherwise subsequently submit the data request.
 * </ul>
 */
public class WeightCheckRequestHandler extends BaseDataRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(WeightCheckRequestHandler.class);

    protected final @NotNull DataRequestHandler next;
    protected final @NotNull DruidWebService webService;
    protected final @NotNull QueryWeightUtil queryWeightUtil;

    /**
     * Build a weight checking request handler.
     *
     * @param next  The request handler to delegate the request to.
     * @param webService  The web service to use for weight checking
     * @param queryWeightUtil  A provider which measures estimated weight against allowed weights.
     * @param mapper  A JSON object mapper, used to parse the JSON response from the weight check.
     */
    public WeightCheckRequestHandler(
            DataRequestHandler next,
            DruidWebService webService,
            QueryWeightUtil queryWeightUtil,
            ObjectMapper mapper
    ) {
        super(mapper);
        this.next = next;
        this.webService = webService;
        this.queryWeightUtil = queryWeightUtil;
    }

    @Override
    public boolean handleRequest(
            final RequestContext context,
            final DataApiRequest request,
            final DruidAggregationQuery<?> druidQuery,
            final ResponseProcessor response
    ) {
        // Heuristic test to let requests with very low estimated cardinality directly through
        if (queryWeightUtil.skipWeightCheckQuery(druidQuery)) {
            return next.handleRequest(context, request, druidQuery, response);
        }

        final WeightCheckResponseProcessor weightCheckResponse = new WeightCheckResponseProcessor(response);
        final DruidAggregationQuery<?> weightEvaluationQuery = queryWeightUtil.makeWeightEvaluationQuery(druidQuery);
        Granularity granularity = druidQuery.getInnermostQuery().getGranularity();
        final long queryRowLimit = queryWeightUtil.getQueryWeightThreshold(granularity);

        try {
            LOG.debug("Weight query {}", writer.writeValueAsString(weightEvaluationQuery));
        } catch (JsonProcessingException e) {
            LOG.warn("Weight Query json exception:", e);
        }

        final SuccessCallback weightQuerySuccess = buildSuccessCallback(
                context,
                request,
                druidQuery,
                weightCheckResponse,
                queryRowLimit
        );
        HttpErrorCallback error = response.getErrorCallback(druidQuery);
        FailureCallback failure = response.getFailureCallback(druidQuery);
        webService.postDruidQuery(context, weightQuerySuccess, error, failure, weightEvaluationQuery);
        return true;
    }

    /**
     * Build a callback which continues the original request or refuses it with an HTTP INSUFFICIENT_STORAGE (507)
     * status based on the cardinality of the requester 's query as measured by the weight check query.
     *
     * @param context  The context data from the request processing chain
     * @param request  The API request itself
     * @param druidQuery  The query being processed
     * @param response  the response handler
     * @param queryRowLimit  The number of aggregating lines allowed
     *
     * @return The callback handler for the weight request
     */
    protected SuccessCallback buildSuccessCallback(
            final RequestContext context,
            final DataApiRequest request,
            final DruidAggregationQuery<?> druidQuery,
            final ResponseProcessor response,
            final long queryRowLimit
    ) {
        return new SuccessCallback() {
            @Override
            public void invoke(JsonNode jsonResult) {
                try {
                    // The result will contain either one result reflecting the row count or
                    // none if the request matches no rows.
                    LOG.debug("{}", writer.writeValueAsString(jsonResult));

                    JsonNode row = jsonResult.get(0);
                    // If the weight limit query is empty or reports acceptable rows, run the full query
                    if (row != null) {
                        int rowCount = row.get("event").get("count").asInt();

                        if (rowCount > queryRowLimit) {
                            String reason = String.format(
                                    ErrorMessageFormat.WEIGHT_CHECK_FAILED.logFormat(rowCount, queryRowLimit),
                                    rowCount,
                                    queryRowLimit
                            );
                            String description = ErrorMessageFormat.WEIGHT_CHECK_FAILED.format();

                            LOG.debug(reason);
                            response.getErrorCallback(druidQuery).dispatch(
                                507, //  Insufficient Storage
                                reason,
                                description
                            );
                            return;
                        }
                    }
                    next.handleRequest(context, request, druidQuery, response);
                } catch (Throwable e) {
                    LOG.info("Exception processing druid call in success", e);
                    response.getFailureCallback(druidQuery).dispatch(e);
                }
            }
        };
    }
}
