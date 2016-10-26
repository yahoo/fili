// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.logging.RequestLog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.StatusType;

/**
 * A static utility class for assisting request processing.
 */
public class RequestHandlerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(RequestHandlerUtils.class);

    /**
     * Builds error response from exception without group by.
     *
     * @param status  the response status
     * @param cause  exception
     * @param writer  The Writer to use for writing errors
     *
     * @return Response
     */
    public static javax.ws.rs.core.Response makeErrorResponse(
            StatusType status,
            Throwable cause,
            ObjectWriter writer
    ) {
        return makeErrorResponse(status, null, cause, writer);
    }

    /**
     * Builds error response from exception.
     *
     * @param status  the response status
     * @param druidQuery  failed Druid Query if available or null
     * @param cause  exception
     * @param writer  The Writer to use for writing errors
     *
     * @return Response
     */
    public static Response makeErrorResponse(
            StatusType status,
            DruidQuery<?> druidQuery,
            Throwable cause,
            ObjectWriter writer
    ) {
        String reason = null;
        String description = null;

        // do not return JAX-RS processing exception, only the cause
        if (cause instanceof ProcessingException && cause.getCause() != null) {
            cause = cause.getCause();
        }

        if (cause != null) {
            reason = cause.getClass().getName();
            description = String.valueOf(cause.getMessage());
        }

        return makeErrorResponse(status.getStatusCode(), reason, description, druidQuery, writer);
    }

    /**
     * Builds error response for Web Service. Example with a groupBy query:
     * <pre><code>
     * {
     *     "status" : 500
     *     "statusName" : "INTERNAL_SERVER_ERROR",
     *     "reason" : "Request failed.",
     *     "description" : "",
     *     "druidQuery" : {
     *         "queryType" : "groupBy",
     *         "dataSource" : { "name" : "slice_pr_ptf_ptfv_us_pf", "type" : "table" },
     *         "dimensions" : [ "login_state","product_region","platform","platform_version"],
     *         "aggregations" : [
     *             { "name" : "pageViews", "fieldName" : "other_page_views", "type" : "longSum" }
     *         ],
     *         "postAggregations" : [ ],
     *         "intervals" : [ "2014-06-01T00:00:00.000Z/2014-06-01T00:00:00.000Z" ],
     *         "granularity" : { "type" : "period", "period" : "P1W" }
     *     }
     * }
     * </code></pre>
     *
     * @param statusCode  the response status code
     * @param reason  error type
     * @param description  error description
     * @param druidQuery  failed Druid Query if available or null
     * @param writer  the writer to use for writing errors
     *
     * @return Response
     */
    public static javax.ws.rs.core.Response makeErrorResponse(
            int statusCode,
            String reason,
            String description,
            DruidQuery<?> druidQuery,
            ObjectWriter writer
    ) {
        Status statusValue = Status.fromStatusCode(statusCode);
        if (Status.Family.REDIRECTION.equals(Status.Family.familyOf(statusCode))) {
            statusValue = Status.INTERNAL_SERVER_ERROR;
            statusCode = statusValue.getStatusCode();
            LOG.warn("Invalid response from Druid: " + statusCode);
        }

        String statusName;
        if (statusValue == null) {
            statusName = Integer.toString(statusCode);
        } else {
            statusName = statusValue.getReasonPhrase();
        }

        LinkedHashMap<String, Object> responseMap = new LinkedHashMap<>();
        responseMap.put("status", statusCode);
        responseMap.put("statusName", statusName);
        responseMap.put("reason", reason);
        responseMap.put("description", description);
        responseMap.put("druidQuery", druidQuery);
        responseMap.put("requestId", RequestLog.getId());

        String json;
        try {
            json = writer.withDefaultPrettyPrinter().writeValueAsString(responseMap);
        } catch (JsonProcessingException e) {
            json = e.getMessage();
        }
        return javax.ws.rs.core.Response.status(statusCode).entity(json).build();
    }
}
