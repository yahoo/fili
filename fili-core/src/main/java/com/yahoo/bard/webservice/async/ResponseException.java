// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.async;

import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;

/**
 * Custom helper class to wrap Error Response object attributes in an Exception type. We need this class when argument
 * type is enforced as Exception.
 */
@SuppressWarnings("serial")
public class ResponseException extends Exception {
    private static final Logger LOG = LoggerFactory.getLogger(ResponseException.class);

    private final int statusCode;
    private final String reason;
    private final String description;
    private final DruidQuery<?> druidQuery;

    /**
     * Class constructor with all the parameters to prepare the error response, plus a writer to serialize the Druid
     * query.
     *
     * @param statusCode  Http status code for the error
     * @param reason  Reason for the error
     * @param description  Description for the error
     * @param druidQuery  The druid query being processed
     * @param cause  Exception object with error details
     * @param objectWriter  The writer to use to serialize the Druid query
     */
    public ResponseException(
            int statusCode,
            String reason,
            String description,
            DruidQuery<?> druidQuery,
            Throwable cause,
            ObjectWriter objectWriter
    ) {
        super(buildMessage(reason, description, statusCode, druidQuery, cause, objectWriter), cause);
        this.statusCode = statusCode;
        this.reason = reason;
        this.description = description;
        this.druidQuery = druidQuery;
    }

    /**
     * Class constructor with throwable, other parameters and a mapper for serializing the druid query.
     *
     * @param statusType  Status type of the response
     * @param druidQuery  The druid query being processed
     * @param error  Exception object with error details
     * @param writer  Writer for serializing the druid query
     */
    public ResponseException(
            Response.StatusType statusType,
            DruidAggregationQuery<?> druidQuery,
            Throwable error,
            ObjectWriter writer
    ) {
        this(
                statusType.getStatusCode(),
                ErrorUtils.getReason(error),
                ErrorUtils.getDescription(error),
                druidQuery,
                error,
                writer
        );
    }

    /**
     * Builds a message for this exception that is basically a straightforward serialization of its interesting state.
     *
     * @param reason  The reason for the error
     * @param description  A description of the error
     * @param statusCode  The status code received from Druid
     * @param druidQuery  The druid query that triggered the invalid response
     * @param cause  The cause of this exception, if any
     * @param objectWriter  The writer to use to serialize the Druid query for the exception message
     *
     * @return A Stringification of the parameters to serve as this exception's message
     */
    private static String buildMessage(
            String reason,
            String description,
            int statusCode,
            DruidQuery<?> druidQuery,
            Throwable cause,
            ObjectWriter objectWriter
    ) {
        String druidQueryString;
        try {
            druidQueryString = objectWriter.writeValueAsString(druidQuery);
        } catch (JsonProcessingException jse) {
            try {
                druidQueryString = druidQuery.toString();
            } catch (Exception e) {
                LOG.warn("Error invoking a druid query's toString.", e);
                druidQueryString = "QUERY'S `toString` FAILED";
            }
            LOG.warn(String.format("Failed to serialize druid query %s", druidQueryString), jse);
        }
        return String.format(
                "reason: %s, description: %s, statusCode: %d, druid query: %s, cause: %s",
                reason,
                description,
                statusCode,
                druidQueryString,
                cause
        );
    }

    public String getReason() {
        return reason;
    }

    public String getDescription() {
        return description;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public DruidQuery<?> getDruidQuery() {
        return druidQuery;
    }
}
