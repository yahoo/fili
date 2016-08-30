// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.jobrows;

/**
 * A job status describes the current state of a job (i.e. 'pending', 'success', 'failure').
 */
public interface JobStatus {

    /**
     * Returns a human-friendly name of the status.
     *
     * @return The name of the status
     */
    String getName();

    /**
     * Returns a human-friendly description of the status.
     *
     * @return A description of the status
     */
    String getDescription();
}
