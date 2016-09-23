// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.payloads;

import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.DATE_CREATED;
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.DATE_UPDATED;
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.JOB_TICKET;
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.QUERY;
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.STATUS;
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.USER_ID;

import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.JobRequestFailedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.UriInfo;

/**
 * The default implementation of JobPayloadBuilder using DefaultJobField to generate the job payload to be returned to
 * the user.
 */
public class DefaultJobPayloadBuilder implements JobPayloadBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultJobPayloadBuilder.class);

    @Override
    public Map<String, String> buildPayload(JobRow jobRow, UriInfo uriInfo) {

        Map<String, String> job = new LinkedHashMap<>();

        String ticket = jobRow.getId();

        job.put(QUERY.getName(), jobRow.get(QUERY));
        job.put("results", JobPayloadBuilder.getResultsUrl(ticket, uriInfo));
        job.put("syncResults", JobPayloadBuilder.getSyncResultsUrl(ticket, uriInfo));
        job.put("self", JobPayloadBuilder.getSelfUrl(ticket, uriInfo));
        job.put(STATUS.getName(), jobRow.get(STATUS));
        job.put(JOB_TICKET.getName(), jobRow.get(JOB_TICKET));
        job.put(DATE_CREATED.getName(), jobRow.get(DATE_CREATED));
        job.put(DATE_UPDATED.getName(), jobRow.get(DATE_UPDATED));
        job.put(USER_ID.getName(), jobRow.get(USER_ID));

        //throw exception if any of the JobFields are missing in the map
        if (job.containsValue(null)) {

            Set<String> missingFields = job.entrySet().stream()
                    .filter(entry -> entry.getValue() == null)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            String msg = ErrorMessageFormat.JOB_MAPPING_FAILED.format(ticket, jobRow.getRowMap(), missingFields);
            LOG.error(msg);
            throw new JobRequestFailedException(msg);
        }

        return job;
    }
}
