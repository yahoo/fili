// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.jobrows;

import com.yahoo.bard.webservice.util.EnumUtils;

/**
 * The default fields for job metadata.
 */
public enum DefaultJobField implements JobField {
    QUERY("The data query this job is satisfying"),
    STATUS("The current status of the job."),
    JOB_TICKET("The job's unique identifier."),
    DATE_CREATED("The date the job was created."),
    USER_ID("The ID of the user that created this job."),
    DATE_UPDATED("The date the job was last updated.");

    private final String serializedName;
    private final String description;

    /**
     * Constructor.
     *
     * @param description  Description of the job field
     */
    DefaultJobField(String description) {
        this.serializedName = EnumUtils.camelCase(name());
        this.description = description;
    }

    @Override
    public String getName() {
        return serializedName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return getName();
    }
}
