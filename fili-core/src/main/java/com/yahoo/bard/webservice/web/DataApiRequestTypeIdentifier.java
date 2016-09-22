// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.SecurityContext;

/**
 * Identifies the type of API request (UI, bypass, CORS pre-flight).
 */
public class DataApiRequestTypeIdentifier {

    public static final String BYPASS_HEADER_NAME = "bard-testing";
    public static final String BYPASS_HEADER_VALUE = "###BYPASS###";

    // Header and value for UI client requests
    public static final String CLIENT_HEADER_NAME = "clientid";
    public static final String CLIENT_HEADER_VALUE = "UI";

    /**
     * Check if the request header is a UI header.
     *
     * @param headers  headers to check
     *
     * @return True if the request is a UI request, false otherwise
     */
    public static boolean isUi(final MultivaluedMap<String, String> headers) {
        // Client header has to be set to the right thing, and the referer(sic) header has to be set
        return CLIENT_HEADER_VALUE.equals(headers.getFirst(CLIENT_HEADER_NAME))
               && headers.get("referer") != null;
    }

    /**
     * Check if the request header is a bypass header.
     *
     * @param headers  headers to check
     *
     * @return True if the request is a bypass request, false otherwise
     */
    public static boolean isBypass(final MultivaluedMap<String, String> headers) {
        // Bypass header has to be set to the right value
        return BYPASS_HEADER_VALUE.equals(headers.getFirst(BYPASS_HEADER_NAME));
    }

    /**
     * Check if the request is a CORS preflight request.
     *
     * @param requestMethod  Request method
     * @param securityContext  Request security context
     *
     * @return True if the request is a CORS preflight request, false otherwise
     */
    public static boolean isCorsPreflight(String requestMethod, SecurityContext securityContext) {
        // Must be an OPTIONS request and not have a user set (since preflight requests should omit auth info)
        return HttpMethod.OPTIONS.equals(requestMethod)
               && (securityContext == null || securityContext.getUserPrincipal() == null);
    }
}
