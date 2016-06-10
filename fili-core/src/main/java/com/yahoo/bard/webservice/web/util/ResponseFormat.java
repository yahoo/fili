// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util;

import com.yahoo.bard.webservice.web.ApiRequest;

import java.util.stream.Collectors;

import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.UriInfo;

/**
 * A utility class for sharing Response logic between metadata and data endpoints.
 */
public class ResponseFormat {
    /**
     * This method will get the path segments and the interval (if it is part of the request) from the apiRequest and
     * create a content-disposition header value with a proposed filename in the following format.
     * <p>
     * If the path segments are ["data", "datasource", "granularity", "dim1"] and the query params have interval
     * {"dateTime": "a/b"}, then the result would be "attachment; filename=data-datasource-granularity-dim1_a_b.csv".
     * For a dimension query without a "dateTime" query param and path segments
     * ["dimensions", "datasource", "dim1"], then the result would be
     * "attachment; filename=dimensions-datasource-dim1.csv".
     *
     * @param apiRequest  The request whose response needs the content disposition header
     *
     * @return A content disposition header telling the browser the name of the CSV file to be downloaded
     */
    public static String getCsvContentDispositionValue(ApiRequest apiRequest) {
        UriInfo uriInfo = apiRequest.getUriInfo();
        String uriPath = uriInfo.getPathSegments().stream()
                .map(PathSegment::getPath)
                .collect(Collectors.joining("-"));

        String interval = uriInfo.getQueryParameters().getFirst("dateTime");
        if (interval == null) {
            interval = "";
        } else {
            interval = "_" + interval.replace("/", "_");
        }

        return "attachment; filename=" + uriPath + interval + ".csv";
    }
}
