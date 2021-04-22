// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.logging.RequestLog;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import javax.annotation.Priority;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MultivaluedMap;

/**
 * The filter which supports CORS security checking.
 */
@Singleton
@Priority(2)
public class ResponseCorsFilter implements ContainerResponseFilter {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
            throws IOException {
        RequestLog.startTiming(this);
        MultivaluedMap<String, Object> headers = responseContext.getHeaders();

        String origin = requestContext.getHeaderString("origin");
        List<String> allowOrigins = getAllowOrigins();

        if (origin == null || "".equals(origin)) {
            // we do not set the Allow-Origin header
        } else if (allowOrigins.isEmpty() || allowOrigins.stream().anyMatch("*"::equals)
                || allowOrigins.stream().anyMatch(origin::equals)) {
            String requestedHeaders = requestContext.getHeaderString("access-control-request-headers");
            headers.putSingle("Access-Control-Allow-Origin", origin);
            headers.putSingle("Access-Control-Allow-Headers", getAllowHeaders(requestedHeaders));
            headers.putSingle("Access-Control-Allow-Methods",
                              getAllowMethods("GET, POST, DELETE, OPTIONS, PUT, PATCH"));
            if (getAllowCredentials(false)) {
                headers.putSingle("Access-Control-Allow-Credentials", "true");
            }
        }
        RequestLog.stopTiming(this);
    }

    /**
     * Gets the list of allow origins.
     *
     * @return List of allow origins
     */
    private List<String> getAllowOrigins() {
        try {
            return SYSTEM_CONFIG.getListProperty(
                    SYSTEM_CONFIG.getPackageVariableName("cors_allow_origins")
            );
        } catch (SystemConfigException ignored) {
            return Collections.emptyList();
        }
    }

    /**
     * Gets the allow headers.
     *
     * @param defaultAllowHeaders default allow headers
     *
     * @return Allow headers
     */
    private String getAllowHeaders(String defaultAllowHeaders) {
        if (defaultAllowHeaders == null) {
            defaultAllowHeaders = "";
        }
        try {
            return SYSTEM_CONFIG.getStringProperty(
                    SYSTEM_CONFIG.getPackageVariableName("cors_allow_headers"),
                    defaultAllowHeaders
            );
        } catch (SystemConfigException ignored) {
            return defaultAllowHeaders;
        }
    }

    /**
     * Gets the allow methods.
     *
     * @param defaultAllowMethods default allow methods
     *
     * @return Allow methods
     */
    private String getAllowMethods(String defaultAllowMethods) {
        try {
            return SYSTEM_CONFIG.getStringProperty(
                    SYSTEM_CONFIG.getPackageVariableName("cors_allow_methods"),
                    defaultAllowMethods
            );
        } catch (SystemConfigException ignored) {
            return defaultAllowMethods;
        }
    }

    /**
     * Gets the allow credentials.
     *
     * @param defaultAllowCredentials default allowed credentials
     *
     * @return Allow credentials
     */
    private boolean getAllowCredentials(boolean defaultAllowCredentials) {
        try {
            return SYSTEM_CONFIG.getBooleanProperty(
                SYSTEM_CONFIG.getPackageVariableName("cors_allow_credentials"),
                defaultAllowCredentials
            );
        } catch (SystemConfigException ignored) {
            return defaultAllowCredentials;
        }
    }
}
