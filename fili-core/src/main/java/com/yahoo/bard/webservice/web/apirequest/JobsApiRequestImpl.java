// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;
import com.yahoo.bard.webservice.async.jobs.payloads.JobPayloadBuilder;
import com.yahoo.bard.webservice.async.jobs.stores.ApiJobStore;
import com.yahoo.bard.webservice.async.jobs.stores.JobRowFilter;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.BadFilterException;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.JobNotFoundException;
import com.yahoo.bard.webservice.web.JobRequestFailedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriInfo;

/**
 * Jobs API Request Implementation binds, validates, and models the parts of a request to the jobs endpoint.
 */
public class JobsApiRequestImpl extends ApiRequestImpl implements JobsApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(JobsApiRequestImpl.class);

    private final JobPayloadBuilder jobPayloadBuilder;
    private final ApiJobStore apiJobStore;
    private final String filters;
    private final UriInfo uriInfo;

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param format  response data format JSON or CSV. Default is JSON.
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
     * @deprecated prefer constructor with downloadFilename
     */
    @Deprecated
    public JobsApiRequestImpl(
            String format,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page,
            String filters,
            UriInfo uriInfo,
            JobPayloadBuilder jobPayloadBuilder,
            ApiJobStore apiJobStore
    ) {
        super(format, asyncAfter, perPage, page);
        this.uriInfo = uriInfo;
        this.jobPayloadBuilder = jobPayloadBuilder;
        this.apiJobStore = apiJobStore;
        this.filters = filters;
    }

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
    public JobsApiRequestImpl(
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
        super(format, downloadFilename, asyncAfter, perPage, page);
        this.uriInfo = uriInfo;
        this.jobPayloadBuilder = jobPayloadBuilder;
        this.apiJobStore = apiJobStore;
        this.filters = filters;
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
        return apiJobStore.get(ticket)
                .switchIfEmpty(
                        Observable.error(new JobNotFoundException(ErrorMessageFormat.JOB_NOT_FOUND.format(ticket)))
                )
                .map(jobRow -> jobPayloadBuilder.buildPayload(jobRow, uriInfo));
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
        try {
            return jobPayloadBuilder.buildPayload(jobRow, uriInfo);
        } catch (JobRequestFailedException ignored) {
            String msg = ErrorMessageFormat.JOBS_RETREIVAL_FAILED.format(jobRow.getId());
            LOG.error(msg);
            throw new JobRequestFailedException(msg);
        }
    }
}
