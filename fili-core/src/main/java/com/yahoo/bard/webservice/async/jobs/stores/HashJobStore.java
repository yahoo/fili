// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.stores;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_OPERATOR_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_JOBFIELD_UNDEFINED;

import com.yahoo.bard.webservice.async.jobs.jobrows.JobField;
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;
import com.yahoo.bard.webservice.web.FilterOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * An ApiJobStore backed by an in-memory map. This is meant as a stub implementation for
 * testing and playing purposes. It is _not_ meant to be used in production. For one, it stores the ticket
 * information in memory, which is not durable. For another, it does not attempt to cleanup sufficiently old jobs,
 * so its memory footprint will grow until the system is rebooted.
 */
public class HashJobStore implements ApiJobStore {

    private static final Logger LOG = LoggerFactory.getLogger(HashJobStore.class);

    private final Map<String, JobRow> store;

    /**
     * Builds a job store using the passed in map as the backing store.
     *
     * @param store  The map to use to store job metadata
     */
    public HashJobStore(Map<String, JobRow> store) {
        this.store = store;
    }

    /**
     * Constructs an empty HashJobStore, using a {@link LinkedHashMap} as the backing store.
     */
    public HashJobStore() {
        this(new LinkedHashMap<>());
    }

    @Override
    public Observable<JobRow> get(String id) {
        JobRow jobRow  = store.get(id);
        return jobRow == null ? Observable.empty() : Observable.just(jobRow);
    }

    @Override
    public Observable<JobRow> save(JobRow metadata) {
        store.put(metadata.getId(), metadata);
        return Observable.just(metadata);
    }

    @Override
    public Observable<JobRow> getAllRows() {
        return Observable.from(store.values());
    }

    @Override
    public Observable<JobRow> getFilteredRows(Set<JobRowFilter> jobRowFilters) throws IllegalArgumentException {
        return getAllRows().filter(jobRow -> satisfiesFilters(jobRowFilters, jobRow));
    }

    /**
     * This method checks if the given JobRow satisfies all the JobRowFilters and returns true if it does.
     * If a JobField in any of the filters is not a part of the JobRow, this method throws an IllegalArgumentException.
     *
     * @param jobRowFilters  A Set of JobRowFilters specifying the different conditions to be satisfied
     * @param jobRow  The JobRow which needs to be inspected
     *
     * @return true if the JobRow satisfies all the filters, false otherwise
     *
     * @throws IllegalArgumentException if a JobField in any of the filters is not a part the JobRow
     */
    private boolean satisfiesFilters(Set<JobRowFilter> jobRowFilters, JobRow jobRow) throws IllegalArgumentException {
        return jobRowFilters.stream().allMatch(filter -> satisfiesFilter(jobRow, filter));
    }

    /**
     * This method checks if the given JobRow satisfies the given JobRowFilter and returns true if it does.
     * If a JobField in the filter is not a part the JobRow, this method throws an IllegalArgumentException.
     *
     * @param jobRow  The JobRow which needs to be inspected
     * @param jobRowFilter  A JobRowFilter specifying the filter condition
     *
     * @return true if the JobRow satisfies the filter, false otherwise
     *
     * @throws IllegalArgumentException if a JobField in the filter is not a part the JobRow
     */
    private boolean satisfiesFilter(JobRow jobRow, JobRowFilter jobRowFilter) throws IllegalArgumentException {
        JobField filterJobField = jobRowFilter.getJobField();
        FilterOperation filterOperation = jobRowFilter.getOperation();
        Set<String> filterValues = jobRowFilter.getValues();

        if (!jobRow.containsKey(filterJobField)) {
            Set<JobField> actualJobFields = jobRow.keySet();
            LOG.debug(FILTER_JOBFIELD_UNDEFINED.logFormat(filterJobField, actualJobFields));
            throw new IllegalArgumentException(
                    FILTER_JOBFIELD_UNDEFINED.format(filterJobField, actualJobFields)
            );
        }

        String actualValue = jobRow.get(filterJobField);

        switch (filterOperation) {
            case notin:
                return !filterValues.contains(actualValue);
            case startswith:
                return filterValues.stream().anyMatch(actualValue::startsWith);
            case contains :
                return filterValues.stream().anyMatch(actualValue::contains);
            case in: // the fall-through is intentional because in is a synonym for eq
            case eq:
                return filterValues.contains(actualValue);
            default:
                LOG.debug(FILTER_OPERATOR_INVALID.logFormat(filterOperation));
                throw new IllegalArgumentException(FILTER_OPERATOR_INVALID.format(filterOperation));
        }
    }
}
