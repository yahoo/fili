// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.payloads;

import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;
import com.yahoo.bard.webservice.web.endpoints.JobsServlet;

import java.util.Map;

import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

/**
 * Interface to build a Job payload from a JobRow.
 */
public interface JobPayloadBuilder {

    /**
     * Given a Map containing job metadata, return a map representing job payload to be sent to the user.
     *
     * @param jobRow  The JobRow containing job metadata
     * @param uriInfo  UriInfo of the Jobs endpoint
     *
     * @return Map representing the job to be returned to the user
     */
    Map<String, String> buildPayload(JobRow jobRow, UriInfo uriInfo);

    /**
     * Get the job results url.
     * <pre>
     *  "http://localhost:9998/jobs/ticket1/results"
     * </pre>
     *
     * @param ticket  The ticket that can uniquely identify a Job
     * @param uriInfo  UriInfo of the request
     *
     * @return The job results url.
     */
     static String getResultsUrl(String ticket, UriInfo uriInfo) {
        return getResultsBaseUrl(uriInfo)
                .build(ticket)
                .toASCIIString();
    }

    /**
     * Get the url for obtaining the job results synchronously.
     * <pre>
     *     "http://localhost:9998/jobs/ticket1/results?asyncAfter=never"
     * </pre>
     *
     * @param ticket  The ticket that can uniquely identify a Job
     * @param uriInfo  UriInfo of the request
     *
     * @return The url for obtaining the job results synchronously
     */
    static String getSyncResultsUrl(String ticket, UriInfo uriInfo) {
        return getResultsBaseUrl(uriInfo)
                .queryParam("asyncAfter", "never")
                .build(ticket)
                .toASCIIString();
    }

    /**
     * Get the UriBuilder for the /jobs/ticket/results endpoint.
     *
     * @param uriInfo  UriInfo of the request
     *
     * @return the UriBuilder for the /jobs/ticket/results endpoint
     */
    static UriBuilder getResultsBaseUrl(UriInfo uriInfo) {
        return uriInfo.getBaseUriBuilder()
                .path(JobsServlet.class)
                .path(JobsServlet.class, "getJobResultsByTicket");
    }

    /**
     * Get the url for the given ticket.
     * <pre>
     *     "http://localhost:9998/jobs/ticket1"
     * </pre>
     *
     * @param ticket  The ticket that can uniquely identify a Job
     * @param uriInfo  UriInfo of the request
     *
     * @return the url for the given ticket
     */
    static String getSelfUrl(String ticket, UriInfo uriInfo) {
        return uriInfo.getBaseUriBuilder()
                .path(JobsServlet.class)
                .path(JobsServlet.class, "getJobByTicket")
                .build(ticket)
                .toASCIIString();
    }
}
