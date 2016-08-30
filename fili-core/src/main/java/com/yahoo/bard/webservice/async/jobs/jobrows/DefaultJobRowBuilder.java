// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.jobrows;

import org.joda.time.DateTime;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

/**
 * A JobRowBuilder that populates a JobRow with the information for all the job fields defined in
 * {@link DefaultJobField}.
 */
public class DefaultJobRowBuilder implements JobRowBuilder {

    private final Function<Map<JobField, String>, String> idGenerator;
    private final Function<SecurityContext, String> userIdExtractor;
    private final Clock timestampGenerator;

    /**
     * Builds a factory for generating JobRows with a custom function for extracting a userid from a request's
     * SecurityContext.
     *
     * @param idGenerator  A function that takes all the job metadata except for the job's id and generates a globally
     * unique id
     * @param userIdExtractor  A function that given a SecurityContext, returns the id of the user who made the request
     * @param timestampGenerator  The clock to use to generate timestamps
     */
    public DefaultJobRowBuilder(
            Function<Map<JobField, String>, String> idGenerator,
            Function<SecurityContext, String> userIdExtractor,
            Clock timestampGenerator
    ) {
        this.idGenerator = idGenerator;
        this.userIdExtractor = userIdExtractor;
        this.timestampGenerator = timestampGenerator;
    }

    /**
     * Builds a factory for generating JobRows containing values for every {@link DefaultJobField}.
     * <p>
     * The user id is extracted from the UserPrincipal of a request's SecurityContext, and timestamps are generated from
     * the System clock.
     *
     * @param idGenerator  A function that takes all the job metadata except for the job's id, and returns a globally
     * unique id for the job
     */
    public DefaultJobRowBuilder(Function<Map<JobField, String>, String> idGenerator) {
        this(
                idGenerator,
                securityContext -> securityContext.getUserPrincipal().getName(),
                Clock.systemDefaultZone()
        );
    }

    /**
     * Builds a JobRow with values for every field defined in {@link DefaultJobField}.
     *
     * @param request  The request that is triggering this job
     * @param requestContext  The context of the request triggering this job
     *
     * @return A JobRow with all the metadata for the job under construction
     */
    @Override
    public JobRow buildJobRow(UriInfo request, ContainerRequestContext requestContext) {
        Map<JobField, String> jobMetadata = new LinkedHashMap<>(DefaultJobField.values().length);

        String userId = userIdExtractor.apply(requestContext.getSecurityContext());

        jobMetadata.put(DefaultJobField.QUERY, request.getRequestUri().toString());

        jobMetadata.put(DefaultJobField.STATUS, DefaultJobStatus.PENDING.getName());

        String isoDateCreated = new DateTime(timestampGenerator.instant().toEpochMilli()).toDateTimeISO().toString();
        jobMetadata.put(DefaultJobField.DATE_CREATED, isoDateCreated);
        jobMetadata.put(DefaultJobField.DATE_UPDATED, isoDateCreated);

        jobMetadata.put(DefaultJobField.USER_ID, userId);
        jobMetadata.put(DefaultJobField.JOB_TICKET, idGenerator.apply(jobMetadata));

        return new JobRow(DefaultJobField.JOB_TICKET, jobMetadata);
    }
}
