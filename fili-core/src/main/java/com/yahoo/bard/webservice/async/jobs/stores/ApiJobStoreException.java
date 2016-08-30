// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.stores;

import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;

/**
 * Class for exception thrown while saving a record in ApiJobStore.
 */
public class ApiJobStoreException extends Exception {
    private final JobRow jobRow;

    /**
     * build an ApiJobStoreException associated with the given JobRow.
     *
     * @param jobRow  The JobRow on which this exception needs to be thrown
     */
    public ApiJobStoreException(JobRow jobRow) {
        this.jobRow = jobRow;
    }

    /**
     * Build an ApiJobStoreException with the cause and the JobRow on which the exception is thrown.
     *
     * @param cause  The Exception that triggered this one
     * @param jobRow  The JobRow the store attempted to save when the exception was triggered
     */
    public ApiJobStoreException(Exception cause, JobRow jobRow) {
        super(cause);
        this.jobRow = jobRow;
    }

    public JobRow getJobRow() {
        return jobRow;
    }
}
