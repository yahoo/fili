// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField;
import com.yahoo.bard.webservice.web.DefaultResponseFormatType;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.ApiRequest;

import java.util.Optional;
import java.util.stream.Collectors;

import javax.swing.text.html.Option;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.UriInfo;

/**
 * A utility class for sharing Response logic between metadata and data endpoints.
 */
public class ResponseUtils {

    public static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    public static final String MAX_NAME_LENGTH = SYSTEM_CONFIG.getPackageVariableName("download_file_max_name_length");

    public static final String CONTENT_DISPOSITION_HEADER_PREFIX = "attachment; filename=";

    protected int maxFileLength = SYSTEM_CONFIG.getIntProperty(MAX_NAME_LENGTH, 0);

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
     * @param containerRequestContext  the state of the container for building response headers
     *
     * @return A content disposition header telling the browser the name of the CSV file to be downloaded
     * @deprecated prefer to use
     */
    @Deprecated
    public String getCsvContentDispositionValue(ContainerRequestContext containerRequestContext) {
        return getContentDispositionValue(containerRequestContext, Optional.empty(), DefaultResponseFormatType.CSV);
    }

    public String getContentDispositionValue(
            ContainerRequestContext containerRequestContext,
            Optional<String> downloadFileName,
            ResponseFormatType responseFormatType
    ) {
        String filepath = truncateFilePath(
                downloadFileName.orElse(prepareDefaultFileNameNoExtension(containerRequestContext))
        );
        String extension = responseFormatType.getFileExtension();
        return CONTENT_DISPOSITION_HEADER_PREFIX + filepath + extension;

    }

    protected String prepareDefaultFileNameNoExtension(ContainerRequestContext containerRequestContext) {
        UriInfo uriInfo = containerRequestContext.getUriInfo();
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
        return uriPath + interval;
    }

    protected String truncateFilePath(String filePath) {
        return (maxFileLength > 0 && filePath.length() > maxFileLength)
                ? filePath.substring(0, maxFileLength)
                : filePath;
    }
}
