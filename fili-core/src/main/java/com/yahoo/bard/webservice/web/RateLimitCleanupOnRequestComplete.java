package com.yahoo.bard.webservice.web;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * Cleans up the resources related to a RateLimiter tracking an outstanding request
 */
public interface RateLimitCleanupOnRequestComplete {

    /**
     * Performs the cleanup
     *
     * @param request  The request that has been completed
     */
    void cleanup(ContainerRequestContext request);
}
