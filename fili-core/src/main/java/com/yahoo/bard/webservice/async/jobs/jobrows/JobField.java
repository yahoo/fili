// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.jobrows;

/**
 * Represents a column in the table of job metadata.
 */
public interface JobField {

    /**
     * Returns the name of the field (i.e. the name of the column in the job metadata table).
     *
     * @return The name of the field
     */
    String getName();

    /**
     * Returns a human-friendly description of the field.
     *
     * @return A human-readable description of the field
     */
    String getDescription();
}
