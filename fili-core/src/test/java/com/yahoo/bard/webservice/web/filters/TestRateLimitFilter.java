// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.config.SystemConfigException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.Principal;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

/**
 * Mocks RateLimitFilter to return mock user from parameter.
 */
public class TestRateLimitFilter extends RateLimitFilter {

    private static final Logger LOG = LoggerFactory.getLogger(RateLimitFilter.class);

    public static final String USER_PARAM = "userParam";

    // gives test access to this filter instance
    static public TestRateLimitFilter instance;

    /**
     * Constructor.
     *
     * @throws SystemConfigException if there's a problem getting the configuration
     */
    public TestRateLimitFilter() throws SystemConfigException {
        super();
        instance = this;
    }

    @Override
    public void filter(final ContainerRequestContext request) throws IOException {
        try {
            super.filter(new MockContext(request));
        } catch (RuntimeException | IOException e) {
            LOG.error("{}", e.toString());
            throw e;
        }
    }

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) throws IOException {
        try {
            super.filter(request, response);
        } catch (RuntimeException | IOException e) {
            LOG.error("{}", e.toString());
            throw e;
        }
    }

    /**
     * Creates request wrapper to inject user name passed in as USER_PARAM.
     */
    static class MockContext implements ContainerRequestContext, SecurityContext, Principal {

        ContainerRequestContext request;

        /**
         * Constructor.
         *
         * @param request  Request to use for building the mock
         */
        MockContext(ContainerRequestContext request) {
            this.request = request;
        }

        // MockPrincipal
        @Override
        public String getName() {
            try {
                return request.getUriInfo().getQueryParameters().get(USER_PARAM).get(0);
            } catch (NullPointerException ignore) {
                return null;
            }
        }

        // MockRequestContext
        // CHECKSTYLE:OFF
        @Override public SecurityContext getSecurityContext() { return getName() == null ? null : this; }
        @Override public Object getProperty(String name) { return request.getProperty(name); }
        @Override public Collection<String> getPropertyNames() { return request.getPropertyNames(); }
        @Override public void setProperty(String name, Object object) { request.setProperty(name, object); }
        @Override public void removeProperty(String name) { request.removeProperty(name); }
        @Override public UriInfo getUriInfo() { return request.getUriInfo(); }
        @Override public void setRequestUri(URI requestUri) { request.setRequestUri(requestUri); }
        @Override public void setRequestUri(URI baseUri, URI requestUri) { request.setRequestUri(baseUri, requestUri); }
        @Override public Request getRequest() { return request.getRequest(); }
        @Override public String getMethod() { return request.getMethod(); }
        @Override public void setMethod(String method) { request.setMethod(method); }
        @Override public MultivaluedMap<String, String> getHeaders() { return request.getHeaders(); }
        @Override public String getHeaderString(String name) { return request.getHeaderString(name); }
        @Override public Date getDate() { return request.getDate(); }
        @Override public Locale getLanguage() { return request.getLanguage(); }
        @Override public int getLength() { return request.getLength(); }
        @Override public MediaType getMediaType() { return request.getMediaType(); }
        @Override public List<MediaType> getAcceptableMediaTypes() { return request.getAcceptableMediaTypes(); }
        @Override public List<Locale> getAcceptableLanguages() { return request.getAcceptableLanguages(); }
        @Override public Map<String, Cookie> getCookies() { return request.getCookies(); }
        @Override public boolean hasEntity() { return request.hasEntity(); }
        @Override public InputStream getEntityStream() { return request.getEntityStream(); }
        @Override public void setEntityStream(InputStream input) { request.setEntityStream(input); }
        @Override public void setSecurityContext(SecurityContext context) { request.setSecurityContext(context); }
        @Override public void abortWith(Response response) { request.abortWith(response); }

        // MockSecurityContext
        @Override public Principal getUserPrincipal() { return this; }
        @Override public boolean isUserInRole(String role) { return false; }
        @Override public boolean isSecure() { return false; }
        @Override public String getAuthenticationScheme() { return null; }
        // CHECKSTYLE:ON
    }
}
