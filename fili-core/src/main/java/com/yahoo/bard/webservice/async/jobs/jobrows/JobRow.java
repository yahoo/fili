// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.jobrows;

import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import javax.validation.constraints.NotNull;

/**
 * A row in the conceptual table defined by the ApiJobStore, containing the metadata about a particular job.
 */
public class JobRow extends LinkedHashMap<JobField, String> {
    private static final Logger LOG = LoggerFactory.getLogger(JobRow.class);

    private final String jobId;

    /**
     * An internal-only constructor that provides the jobId directly, used by the copy-and-modify methods.
     *
     * @param jobId  The unique identifier for this job
     * @param fieldValueMap  The metadata about this job
     *
     * @throws IllegalArgumentException if jobId is not a key in fieldValueMap
     */
    protected JobRow(@NotNull String jobId, Map<JobField, String> fieldValueMap) {
        super(fieldValueMap);
        if (jobId == null) {
            LOG.error(String.format(ErrorMessageFormat.MISSING_JOB_ID.getLoggingFormat(), fieldValueMap));
            throw new IllegalArgumentException(ErrorMessageFormat.MISSING_JOB_ID.format(fieldValueMap));
        }
        this.jobId = jobId;
    }

    /**
     * Builds a row of job metadata.
     *
     * @param jobIdFieldName  The field used as a key into the row
     * @param fieldValueMap  A mapping from job fields to job metadata values, must contain the jobId as a key
     *
     * @throws IllegalArgumentException if jobId is not a key in fieldValueMap
     */
    public JobRow(@NotNull JobField jobIdFieldName, @NotNull Map<JobField, String> fieldValueMap) {
        this(fieldValueMap.get(jobIdFieldName), fieldValueMap);
    }

    /**
     * Coerces the JobRow into a mapping from the names of JobFields to their values.
     *
     * @return A mapping from the name of each JobField to its associated value
     */
    public Map<String, String> getRowMap() {
        return entrySet().stream()
                .collect(StreamUtils.toLinkedMap(entry -> entry.getKey().getName(), Map.Entry::getValue));
    }

    /**
     * Returns a copy of this JobRow with the specified field set to the specified value.
     *
     * @param field  The field to set
     * @param value  The value to set the field to
     *
     * @return A new JobRow identical this one, except for the specified field
     */
    public JobRow withFieldValue(JobField field, String value) {
        JobRow newRow = new JobRow(jobId, new LinkedHashMap<>(this));
        newRow.put(field, value);
        return newRow;
    }

    @JsonIgnore
    public String getId() {
        return jobId;
    }

    /**
     * The hash code of a job row is simply the hash code of its ID.
     *
     * @return The hash code of the job row
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(getId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        JobRow jobRow = (JobRow) o;

        return jobId.equals(jobRow.jobId) && super.equals(o);
    }

    @Override
    public String toString() {
        return String.join("'", "{jobId=", jobId, ", entries=", entrySet().toString(), "}");
    }
}
