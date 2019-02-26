// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.web.util.BardConfigResources;

import java.util.List;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.PathSegment;

/**
 * Layer of injection to provide preprocessing of BardConfig in response to request parameters and transformation of
 * request parameters before performing binding in the ApiRequest constructors.
 */
public interface DataApiRequestFactory {

    /**
     * Factory method for building {@link DataApiRequest} objects.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  string time granularity in URL
     * @param dimensions  single dimension or multiple dimensions separated by '/' in URL
     * @param logicalMetrics  URL logical metric query string in the format:
     * <pre>{@code single metric or multiple logical metrics separated by ',' }</pre>
     * @param intervals  URL intervals query string in the format:
     * <pre>{@code single interval in ISO 8601 format, multiple values separated by ',' }</pre>
     * @param apiFilters  URL filter query String
     * @param havings  URL having query String
     * @param sorts  string of sort columns along with sort direction
     * @param count  count of number of records to be returned in the response
     * @param topN  number of first records per time bucket to be returned in the response
     * @param format  response data format
     * @param timeZoneId  a joda time zone id
     * @param asyncAfter  How long the user is willing to wait for a synchronous request in milliseconds
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param bardConfigResources  The configuration resources used to build this api request
     *
     * @return A DataApiRequestImpl instance
     * @deprecated in favor of the version that uses the filename parameter
     */
    @Deprecated
    DataApiRequest buildApiRequest(
            String tableName,
            String granularity,
            List<PathSegment> dimensions,
            String logicalMetrics,
            String intervals,
            String apiFilters,
            String havings,
            String sorts,
            String count,
            String topN,
            String format,
            String timeZoneId,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page,
            BardConfigResources bardConfigResources
    );

    /**
     * Factory method for building {@link DataApiRequest} objects.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  string time granularity in URL
     * @param dimensions  single dimension or multiple dimensions separated by '/' in URL
     * @param logicalMetrics  URL logical metric query string in the format:
     * <pre>{@code single metric or multiple logical metrics separated by ',' }</pre>
     * @param intervals  URL intervals query string in the format:
     * <pre>{@code single interval in ISO 8601 format, multiple values separated by ',' }</pre>
     * @param apiFilters  URL filter query String
     * @param havings  URL having query String
     * @param sorts  string of sort columns along with sort direction
     * @param count  count of number of records to be returned in the response
     * @param topN  number of first records per time bucket to be returned in the response
     * @param format  response data format
     * @param timeZoneId  a joda time zone id
     * @param asyncAfter  How long the user is willing to wait for a synchronous request in milliseconds
     * @param downloadFilename  The filename for the response to be downloaded as. If null indicates response should
     * not be downloaded.
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param bardConfigResources  The configuration resources used to build this api request
     *
     * @return A DataApiRequestImpl instance
     */
    DataApiRequest buildApiRequest(
            String tableName,
            String granularity,
            List<PathSegment> dimensions,
            String logicalMetrics,
            String intervals,
            String apiFilters,
            String havings,
            String sorts,
            String count,
            String topN,
            String format,
            String downloadFilename,
            String timeZoneId,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page,
            BardConfigResources bardConfigResources
    );
}
