// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util;

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
     * @param uriInfo  UriInfo of the request
     *
     * @return A content disposition header telling the browser the name of the CSV file to be downloaded
     */
    public static String getCsvContentDispositionValue(UriInfo uriInfo) {
        String uriPath = uriInfo.getPathSegments().stream()
                .map(PathSegment::getPath)
                .collect(Collectors.joining("-"));

        String interval = uriInfo.getQueryParameters().getFirst("dateTime");
        if (interval == null) {
            interval = "";
        } else {
            // Chrome treats ',' as duplicate header so replace it with '__' to make chrome happy.
            interval = "_" + interval.replace("/", "_").replace(",", "__");
        }

        return "attachment; filename=" + uriPath + interval + ".csv";
    }
}
