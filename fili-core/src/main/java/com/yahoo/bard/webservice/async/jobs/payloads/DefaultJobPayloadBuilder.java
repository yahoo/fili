// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.payloads;

import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField;
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
        Map<String, String> fieldValueMap = jobRow.getRowMap();

        job.put("query", fieldValueMap.get(DefaultJobField.QUERY.getName()));
        job.put("results", JobPayloadBuilder.getResultsUrl(ticket, uriInfo));
        job.put("syncResults", JobPayloadBuilder.getSyncResultsUrl(ticket, uriInfo));
        job.put("self", JobPayloadBuilder.getSelfUrl(ticket, uriInfo));
        job.put("status", fieldValueMap.get(DefaultJobField.STATUS.getName()));
        job.put("jobTicket", fieldValueMap.get(DefaultJobField.JOB_TICKET.getName()));
        job.put("dateCreated", fieldValueMap.get(DefaultJobField.DATE_CREATED.getName()));

        //throw exception if any of the JobFields are missing in the map
        if (job.containsValue(null)) {

            Set<String> missingFields = job.entrySet().stream()
                    .filter(entry -> entry.getValue() == null)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            String msg = ErrorMessageFormat.JOB_MAPPING_FAILED.format(ticket, fieldValueMap, missingFields);
            LOG.error(msg);
            throw new JobRequestFailedException(msg);
        }

        return job;
    }
}
