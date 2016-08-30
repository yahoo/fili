// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.jobrows;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.UriInfo;

/**
 * Interface for building JobRows (i.e. metadata about an asynchronous job) from Bard requests. Provides an injection
 * point that allows customers to build custom job metadata.
 */
public interface JobRowBuilder {

    /**
     * Builds the bean holding the metadata about an asynchronous job.
     *
     * @param request  The request that is triggering this job
     * @param requestContext  The context of the request triggering this job
     *
     * @return A bean containing the metadata about this job
     */
    JobRow buildJobRow(UriInfo request, ContainerRequestContext requestContext);
}
