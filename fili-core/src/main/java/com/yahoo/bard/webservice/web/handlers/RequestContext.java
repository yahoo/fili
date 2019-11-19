// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.util.Utils;

import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriBuilder;

/**
 * A container for state gathered by the web container and used to handle requests.
 */
public class RequestContext {

    protected final ContainerRequestContext containerRequestContext;
    protected final boolean readCache;
    protected final MultivaluedMap<String, String> searchableHeaders;
    protected final AtomicLong numberOfIncoming = new AtomicLong(1);
    protected final AtomicLong numberOfOutgoing = new AtomicLong(1);

    /**
     * Build a context for a request.
     *
     * @param containerRequestContext  context from the http request object
     * @param readCache  true if the cache should be checked for a response
     */
    public RequestContext(ContainerRequestContext containerRequestContext, boolean readCache) {
        this.containerRequestContext = containerRequestContext;
        this.readCache = readCache;
        this.searchableHeaders = containerRequestContext != null ?
                Utils.headersToLowerCase(containerRequestContext.getHeaders()) :
                new MultivaluedHashMap<>();
    }

    public MultivaluedMap<String, String> getHeaders() {
        return containerRequestContext.getHeaders();
    }

    public MultivaluedMap<String, String> getHeadersLowerCase() {
        return searchableHeaders;
    }

    public SecurityContext getSecurityContext() {
        return containerRequestContext.getSecurityContext();
    }

    /**
     * Get a container property from the RequestContext.
     *
     * @param name  Name of the property
     * @param <T>  Type of the property to get
     *
     * @return the property
     */
    public <T> T getContainerRequestContextProperty(String name) {
        @SuppressWarnings("unchecked")
        T property = (T) containerRequestContext.getProperty(name);
        return property;
    }

    public boolean isReadCache() {
        return readCache;
    }

    public AtomicLong getNumberOfIncoming() {
        return numberOfIncoming;
    }

    public AtomicLong getNumberOfOutgoing() {
        return numberOfOutgoing;
    }

    public UriBuilder getUriBuilder() {
        return containerRequestContext.getUriInfo().getRequestUriBuilder();
    }
}
