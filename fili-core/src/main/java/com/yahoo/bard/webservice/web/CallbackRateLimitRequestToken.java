package com.yahoo.bard.webservice.web;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * Request token that takes a rateLimitCleanup object on creation, and calls that to handle the cleanup when the request
 * transaction is completed
 */
public class CallbackRateLimitRequestToken implements RateLimitRequestToken {
    RateLimitCleanupOnRequestComplete rateLimitCleanup;
    ContainerRequestContext request;
    boolean isBound;

    CallbackRateLimitRequestToken(RateLimitCleanupOnRequestComplete rateLimitCleanup, ContainerRequestContext request) {
        this.rateLimitCleanup = rateLimitCleanup;
        this.request = request;
        this.isBound = true;
    }

    @Override
    public boolean isBound() {
        return isBound;
    }

    @Override
    public boolean bind() {
        return isBound;
    }

    @Override
    public void unBind() {
        close();
    }

    @Override
    public void close() {
        rateLimitCleanup.cleanup(request);
        this.isBound = false;
    }
}
