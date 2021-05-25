// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.mapimpl;

import com.yahoo.bard.webservice.async.jobs.stores.ApiJobStore;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriInfo;

/**
 * Utilities to aid in converting to Map based ApiRequests.
 */
public class MapRequestUtil {

    /**
     * Convert from old constructor parameters.
     *
     * @param format  A string describing a response format.
     * @param downloadFilename  The filename to coerce the response to.
     * @param asyncAfter The time to wait before switching to asynchronous processing.
     * @param perPage The number of results per page.
     * @param page The page number.
     *
     * @return a map describing the parameter values.
     *
     */
    public static Map<String, String> apiConstructorConverter(
            String format,
            String downloadFilename,
            String asyncAfter,
            String perPage,
            String page
    ) {
        HashMap<String, String> params = new HashMap<>();
        params.computeIfAbsent(ApiRequestMapImpl.FORMAT_KEY, val -> format);
        params.computeIfAbsent(ApiRequestMapImpl.FILENAME_KEY, val -> downloadFilename);
        params.computeIfAbsent(ApiRequestMapImpl.ASYNC_AFTER_KEY, val -> asyncAfter);
        params.computeIfAbsent(ApiRequestMapImpl.PER_PAGE, val -> perPage);
        params.computeIfAbsent(ApiRequestMapImpl.PAGE, val -> page);
        return params;
    }

    /**
     * Convert parameters for jobsApiRequestConstructor.
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
     *
     * @return A map to configure a JobsApiRequest
     */
    public static Map<String, String> jobApiRequestParameters(
            String format,
            String downloadFilename,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page,
            String filters
    ) {
        Map<String, String> params = apiConstructorConverter(
                format,
                downloadFilename,
                asyncAfter,
                perPage,
                page
        );
        params.put(JobsApiRequestMapImpl.FILTERS_KEY, filters);
        return params;
    }

    /**
     * Convert resources for jobsApiRequestConstructor.
     *
     * @param uriInfo  The URI of the request object.
     * @param apiJobStore  The ApiJobStore containing Job metadata
     *
     * @return A map to configure a JobsApiRequest
     */
    public static Map<String, Object> jobApiRequestResources(
            UriInfo uriInfo,
            ApiJobStore apiJobStore
    ) {
        Map<String, Object> resources = new LinkedHashMap<>();
        resources.put(JobsApiRequestMapImpl.URI_INFO, uriInfo);
        resources.put(JobsApiRequestMapImpl.API_JOB_STORE_KEY, apiJobStore);
        return resources;
    }
}
