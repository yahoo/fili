// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.jobrows;

import java.util.Locale;

/**
 * Provides an enumeration of the standard job statuses that Bard supports.
 */
public enum DefaultJobStatus implements JobStatus {
    PENDING("The job is in progress."),
    SUCCESS("The job has completed successfully."),
    CANCELED("The job has been canceled before it could be completed."),
    FAILURE("An error occurred, and the job failed to complete.");

    private final String description;
    private final String name;

    /**
     * Constructor.
     *
     * @param description  Description for the job status
     */
    DefaultJobStatus(String description) {
        this.description = description;
        this.name = name().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getName() {
        return name;
    }
}
