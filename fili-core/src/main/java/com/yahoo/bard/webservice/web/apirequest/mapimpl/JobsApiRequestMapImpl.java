// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.mapimpl;

import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;
import com.yahoo.bard.webservice.async.jobs.payloads.DefaultJobPayloadBuilder;
import com.yahoo.bard.webservice.async.jobs.payloads.JobPayloadBuilder;
import com.yahoo.bard.webservice.async.jobs.stores.ApiJobStore;
import com.yahoo.bard.webservice.async.jobs.stores.JobRowFilter;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.JobNotFoundException;
import com.yahoo.bard.webservice.web.JobRequestFailedException;
import com.yahoo.bard.webservice.web.apirequest.JobsApiRequest;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadFilterException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriInfo;

/**
 * Jobs API Request Implementation binds, validates, and models the parts of a request to the jobs endpoint.
 */
public class JobsApiRequestMapImpl extends ApiRequestMapImpl implements JobsApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(JobsApiRequestMapImpl.class);

    public static final String FILTERS_KEY = "filters";

    public static final String JOB_PAYLOAD_BUILDER_KEY = "jobPayloadBuilder";
    public static final String API_JOB_STORE_KEY = "apiJobStore";
    public static final String URI_INFO = "uriInfo";

    public static final JobPayloadBuilder DEFAULT_FACTORY = new DefaultJobPayloadBuilder();

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param downloadFilename If not null and not empty, indicates the response should be downloaded by the client with
     * the provided filename. Otherwise indicates the response should be rendered in the browser.
     * @param asyncAfter  How long the user is willing to wait for a synchronous request in milliseconds
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param filters  URL filter query String in the format:
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param uriInfo  The URI of the request object.
     * @param jobPayloadBuilder  The JobRowMapper to be used to map JobRow to the Job returned by the api
     * @param apiJobStore  The ApiJobStore containing Job metadata
     */
    public JobsApiRequestMapImpl(
            String format,
            String downloadFilename,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page,
            String filters,
            UriInfo uriInfo,
            JobPayloadBuilder jobPayloadBuilder,
            ApiJobStore apiJobStore
    ) {
        super(MapRequestUtil.apiConstructorConverter(format, downloadFilename, asyncAfter, perPage, page));
        setRequestParameter(FILTERS_KEY, filters);
        putResource(URI_INFO, uriInfo);
        putBinder(JOB_PAYLOAD_BUILDER_KEY, jobPayloadBuilder);
        putResource(API_JOB_STORE_KEY, apiJobStore);
    }

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param parameters  Request parameters
     * @param resources Resource objects
     * @param jobPayloadBuilder The builder for the jobs api request
     */
    public JobsApiRequestMapImpl(
            Map<String, String> parameters,
            Map<String, Object> resources,
            JobPayloadBuilder jobPayloadBuilder
    ) {
        super(parameters, resources, Collections.singletonMap(JOB_PAYLOAD_BUILDER_KEY, jobPayloadBuilder));
    }

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param parameters  Request parameters
     * @param resources Resource objects
     */
    public JobsApiRequestMapImpl(
            Map<String, String> parameters,
            Map<String, Object> resources
    ) {
        this(parameters, resources, DEFAULT_FACTORY);
    }

    /**
     * Returns an Observable over the Map representing the job to be returned to the user.
     *
     * @param ticket  The ticket that uniquely identifies the job
     *
     * @return An Observable over the Map representing the job to be returned to the user or an Observable wrapping
     * JobNotFoundException if the Job is not available in the ApiJobStore
     */
    public Observable<Map<String, String>> getJobViewObservable(String ticket) {
        ApiJobStore apiJobStore = (ApiJobStore) getResource(API_JOB_STORE_KEY);
        JobPayloadBuilder builder = (JobPayloadBuilder) getBinder(JOB_PAYLOAD_BUILDER_KEY);
        UriInfo uriInfo = (UriInfo) getResource(URI_INFO);
        return apiJobStore.get(ticket)
                .switchIfEmpty(
                        Observable.error(new JobNotFoundException(ErrorMessageFormat.JOB_NOT_FOUND.format(ticket)))
                )
                .map(jobRow -> builder.buildPayload(jobRow, uriInfo));
    }

    /**
     * Return an Observable containing a stream of job views for jobs in the ApiJobStore. If filter String is non null
     * and non empty, only return results that satisfy the filter. If filter String is null or empty, return all rows.
     * If, for any JobRow, the mapping from JobRow to job view fails, an Observable over JobRequestFailedException is
     * returned. If the ApiJobStore is empty, we return an empty Observable.
     *
     * @return An Observable containing a stream of Maps representing the job to be returned to the user
     */
    public Observable<Map<String, String>> getJobViews() {
        ApiJobStore apiJobStore = (ApiJobStore) getResource(API_JOB_STORE_KEY);
        String filters = getRequestParameter(FILTERS_KEY);

        Observable<JobRow> rows;
        if (filters == null || "".equals(filters)) {
            rows = apiJobStore.getAllRows();
        } else {
            rows = apiJobStore.getFilteredRows(buildJobStoreFilter(filters));
        }
        return rows.map(this::mapJobRowsToJobViews);
    }

    /**
     * Given a filter String, generates a Set of ApiJobStoreFilters. This method will throw a BadApiRequestException if
     * the filter String cannot be parsed into ApiJobStoreFilters successfully.
     *
     * @param filterQuery  Expects a URL filterQuery String that may contain multiple filters separated by
     * comma.  The format of a filter String is :
     * (JobField name)-(operation)[(value or comma separated values)]?
     *
     * @return  A Set of ApiJobStoreFilters
     */
    public LinkedHashSet<JobRowFilter> buildJobStoreFilter(@NotNull String filterQuery) {
        // split on '],' to get list of filters
        return Arrays.stream(filterQuery.split(COMMA_AFTER_BRACKET_PATTERN))
                .map(
                        filter -> {
                            try {
                                return new JobRowFilter(filter);
                            } catch (BadFilterException e) {
                                throw new BadApiRequestException(e.getMessage(), e);
                            }
                        }
                )
                .collect(Collectors.toCollection(LinkedHashSet<JobRowFilter>::new));
    }

    /**
     * Given a JobRow, map it to the Job payload to be returned to the user. If the JobRow cannot be successfully
     * mapped to a Job View, JobRequestFailedException is thrown.
     *
     * @param jobRow  The JobRow to be mapped to job payload
     *
     * @return Job payload to be returned to the user
     */
    private Map<String, String> mapJobRowsToJobViews(JobRow jobRow) {
        JobPayloadBuilder builder = (JobPayloadBuilder) getBinder(JOB_PAYLOAD_BUILDER_KEY);
        UriInfo uriInfo = (UriInfo) getResource(URI_INFO);

        try {
            return builder.buildPayload(jobRow, uriInfo);
        } catch (JobRequestFailedException ignored) {
            String msg = ErrorMessageFormat.JOBS_RETREIVAL_FAILED.format(jobRow.getId());
            LOG.error(msg);
            throw new JobRequestFailedException(msg);
        }
    }
}
