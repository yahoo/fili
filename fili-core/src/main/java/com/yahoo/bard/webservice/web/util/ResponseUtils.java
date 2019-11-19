// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.web.DefaultResponseFormatType;
import com.yahoo.bard.webservice.web.ResponseFormatType;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.UriInfo;

/**
 * A utility class for sharing Response logic between metadata and data endpoints.
 */
public class ResponseUtils {

    public static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String MAX_NAME_LENGTH = SYSTEM_CONFIG.getPackageVariableName("download_file_max_name_length");
    public static final String CONTENT_DISPOSITION_HEADER_PREFIX = "attachment; filename=";

    protected static final Pattern SLASHES = Pattern.compile("[\\\\\\/]");
    protected static final Pattern COMMA = Pattern.compile(",");

    protected static final String SINGLE_UNDERSCORE = "_";
    protected static final String DOUBLE_UNDERSCORE = "__";

    public static final Collection<ResponseFormatType> DEFAULT_ALWAYS_DOWNLOAD_FORMATS =
            Collections.singleton(DefaultResponseFormatType.CSV);

    protected int maxFileLength = SYSTEM_CONFIG.getIntProperty(MAX_NAME_LENGTH, 0);

    protected final Collection<ResponseFormatType> alwaysDownloadFormats;

    /**
     * Constructor. By default the CSV format is a format that is always returned as an attachment instead of rendered
     * in the browser.
     */
    public ResponseUtils() {
        this(DEFAULT_ALWAYS_DOWNLOAD_FORMATS);
    }

    /**
     * Set of format that should always be returned as an attachment.
     *
     * @param alwaysDownloadFormats the set of formats
     */
    public ResponseUtils(Collection<ResponseFormatType> alwaysDownloadFormats) {
        this.alwaysDownloadFormats = Collections.unmodifiableCollection(
                new HashSet<>(alwaysDownloadFormats)
        );
    }

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
     * @deprecated prefer to use buildResponseFormatHeaders() or at least getContentDispositionValue()
     */
    @Deprecated
    public String getCsvContentDispositionValue(ContainerRequestContext containerRequestContext) {
        return getContentDispositionValue(containerRequestContext, DefaultResponseFormatType.CSV);
    }

    /**
     * Gets the response headers. Convenience method for if there is no user provided download filename.
     *
     * @param containerRequestContext  the container request context of the request currently being handled
     * @param responseFormatType  the response format type for that request.
     * @return A map of applicable headers to values.
     */
    public Map<String, String> buildResponseFormatHeaders(
            ContainerRequestContext containerRequestContext,
            ResponseFormatType responseFormatType
    ) {
        return buildResponseFormatHeaders(containerRequestContext, null, responseFormatType);
    }

    /**
     * Returns a map of all response headers having to do with the response format. Currently this includes the
     * Content-Type header and the Content-Disposition header. The Content-Type header is always returned, but the
     * Content-Disposition header is only returned if the response is supposed to be downloaded.
     *
     * Whether or not the response should be downloaded is based on two pieces of data. First, if a user defined
     * filename is present then the response will always be downloaded. Intuitively, it would only make sense to
     * provide a filename if you intend for the results to be downloaded into a file of that name. The second case is
     * if the provided response format type is considered to be an always download format type. A format type is
     * considered always download if it is in the alwaysDownloadFormats set. If the response format type is an
     * always download format type AND no download filename is provide a default filename is generated based on the
     * path elements and time range provided in the api query.
     *
     * @param containerRequestContext the container request context of the request currently being handled
     * @param downloadFilename  the filename for the response to be downloaded as
     * @param responseFormatType the response format type for that request.
     * @return A map of applicable headers to values.
     */
    public Map<String, String> buildResponseFormatHeaders(
            ContainerRequestContext containerRequestContext,
            String downloadFilename,
            ResponseFormatType responseFormatType
    ) {
        Map<String, String> result = new HashMap<>();
        result.put(HttpHeaders.CONTENT_TYPE, getContentTypeValue(responseFormatType));
        // if the response format is CSV we ALWAYS respond with a
        // if filename is present and not empty then the druid response should be sent back as an attachment
        if (
                alwaysDownloadFormats.contains(responseFormatType) ||
                        downloadFilename != null && !downloadFilename.isEmpty()
                ) {
            result.put(
                    HttpHeaders.CONTENT_DISPOSITION,
                    getContentDispositionValue(containerRequestContext, downloadFilename, responseFormatType)
            );
        }
        return result;
    }

    /**
     * Builds the value for the Content-Type header. The value is constructed out of data provided by the
     * Response Format Type.
     *
     * @param responseFormatType  data object that contains information necessary to build the response headers
     * @return the value for the Content-Type header
     */
    public String getContentTypeValue(ResponseFormatType responseFormatType) {
        return responseFormatType.getContentType() + "; charset=" + responseFormatType.getCharset();
    }

    /**
     * Builds the value for the Content-Disposition header. convenience method for when there is no user provided
     * download filename. A default filename is generated and used instead.
     *
     * @param containerRequestContext  the container request context of the request currently being handled
     * @param responseFormatType  the response format type for that request.
     * @return the value for the Content-Disposition header
     */
    public String getContentDispositionValue(
            ContainerRequestContext containerRequestContext,
            ResponseFormatType responseFormatType
    ) {
        return getContentDispositionValue(
                containerRequestContext,
                generateDefaultFileNameNoExtension(containerRequestContext),
                responseFormatType
        );
    }

    /**
     * Builds the value for the Content-Disposition header. If downloadFilename is null or empty a default name is
     * generated instead.
     *
     * @param containerRequestContext  container request context of the request currently being handled
     * @param requestedFilename  the filename for the response to be downloaded as
     * @param responseFormatType  data object that contains information necessary to build the response headers
     * @return the value for the Content-Disposition header
     */
    public String getContentDispositionValue(
            ContainerRequestContext containerRequestContext,
            String requestedFilename,
            ResponseFormatType responseFormatType
    ) {
        String downloadFilename = requestedFilename;
        if (requestedFilename == null || requestedFilename.isEmpty()) {
            downloadFilename = generateDefaultFileNameNoExtension(containerRequestContext);
        }
        downloadFilename = replaceReservedCharacters(downloadFilename);
        String filepath = removeDuplicateExtensions(containerRequestContext, downloadFilename, responseFormatType);
        filepath = truncateFilename(filepath);
        String extension = responseFormatType.getFileExtension();
        return CONTENT_DISPOSITION_HEADER_PREFIX + filepath + extension;

    }

    /**
     * This method will get the path segments and the interval (if it is part of the request) from the api request and
     * generate a default filename to be used with the content-disposition header.
     * <p>
     * If the path segments are ["data", "datasource", "granularity", "dim1"] and the query params have interval
     * {"dateTime": "a/b"}, then the result would be "attachment; filename=data-datasource-granularity-dim1_a_b.csv".
     * For a dimension query without a "dateTime" query param and path segments
     * ["dimensions", "datasource", "dim1"], then the result would be
     * "attachment; filename=dimensions-datasource-dim1.csv".
     * </p>
     *
     * @param containerRequestContext  container request context of the request currently being handled
     * @return the generated default filename
     */
    protected String generateDefaultFileNameNoExtension(ContainerRequestContext containerRequestContext) {
        UriInfo uriInfo = containerRequestContext.getUriInfo();
        String uriPath = uriInfo.getPathSegments().stream()
                .map(PathSegment::getPath)
                .collect(Collectors.joining("-"));

        String interval = uriInfo.getQueryParameters().getFirst("dateTime");
        if (interval == null) {
            interval = "";
        } else {
            interval = "_" + replaceReservedCharacters(interval);
        }
        return uriPath + interval;
    }

    /**
     * truncates the provided filename if it exceeds the maximum filename length. A maxFileLength of zero indicates
     * no maximum file length is configured and thus the filename will not be truncated.
     *
     * @param filename  the filename to maybe truncate
     * @return the filename truncated to the configured maximum length if necessary
     */
    protected String truncateFilename(String filename) {
        return maxFileLength > 0 && filename.length() > maxFileLength
                ? filename.substring(0, maxFileLength)
                : filename;
    }

    /**
     * If the user provided filename ends with a file extension (or multiple file extensions) that match the file
     * extension of provided by {@link ResponseFormatType#getFileExtension()}, those file extensions are truncated.
     *
     * The comparison is case insensitive, so .CSV will match .csv.
     *
     * If the filename is empty once the extensions are truncated the default filename is generated and returned.
     *
     * @param context  The request context. Used to build the default filename if necessary.
     * @param fileName  The requested filename.
     * @param type  The response type.
     * @return the truncated filename
     */
    protected String removeDuplicateExtensions(
            ContainerRequestContext context,
            String fileName,
            ResponseFormatType type
    ) {
        String truncatedFileName = fileName;
        while (
                truncatedFileName
                        .toLowerCase(Locale.ENGLISH)
                        .endsWith(type.getFileExtension().toLowerCase(Locale.ENGLISH))
        ) {
            truncatedFileName = truncatedFileName.substring(
                    0,
                    truncatedFileName.length() - type.getFileExtension().length()
            );
        }

        if (truncatedFileName.isEmpty()) {
            return generateDefaultFileNameNoExtension(context);
        }

        return truncatedFileName;
    }

    /**
     * Replaces a small set of illegal characters with underscores.
     *
     * '/' and '\' are file path delimiters in unix and windows respectively, so they must be replaced. Chrome treats
     * ',' as duplicate header so it must also be replaced to make chrome happy.
     *
     * @param str  Input to perform replace on
     * @return the input with reserved characters replaced with underscores
     */
    protected String replaceReservedCharacters(String str) {
        str = SLASHES.matcher(str).replaceAll(SINGLE_UNDERSCORE);
        return COMMA.matcher(str).replaceAll(DOUBLE_UNDERSCORE);
    }
}
