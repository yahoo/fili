// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.stores;

import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;

import rx.Observable;

import java.util.Set;

/**
 * An ApiJobStore is responsible for storing the metadata about Bard jobs. Conceptually, the ApiJobStore is a table
 * where each row is the metadata of a particular job, and the columns are the metadata stored with each job
 * (such metadata may include date created, a link to results, etc). The table uses the job's id as the primary
 * key.
 */
public interface ApiJobStore {

    /**
     * Returns a cold Observable that emits 0 or 1 messages: the desired JobRow, or nothing if there is no JobRow with
     * the specified id.
     *
     * <p>
     * If an error is encountered while interacting with the metadata store, then the Observable's {@code onError} is
     * called with the exception as payload.
     *
     * @param id  The ID of the job desired
     *
     * @return An Observable that emits the metadata of the job with the specified id, if such a job exists
     */
    Observable<JobRow> get(String id);

    /**
     * Returns a cold Observable that emits the JobRow that has been stored.
     * The message is not emitted until the metadata has been successfully stored.
     * <p>
     * While the Observable should not emit a message until at least one subscriber has subscribed, it should store
     * the results immediately, without waiting for a subscriber.
     * If a row with the same ID already exists, then that row will be overwritten. Otherwise, a new ID-JobRow
     * mapping is added to the store. If an error is encountered while storing the data, the Observable instead invokes
     * the {@code onError} methods on its subscribers with an {@link ApiJobStoreException}
     *
     * @param metadata  The Job metadata that needs to be stored
     *
     * @return An Observable that emits the JobRow that was just stored
     */
    Observable<JobRow> save(JobRow metadata);

    /**
     * A cold observable that emits a stream of JobRows until all JobRows have been retrieved from the store.
     * If at any time an error is encountered, {@code onError} is invoked, and the stream halts.
     *
     * @return An Observable that emits a stream of all the JobRows in the store
     */
    Observable<JobRow> getAllRows();

    /**
     * This method takes a Set of JobRowFilters, ANDS them by default, and returns a cold observable that emits a
     * stream of JobRows which satisfy the given filter.
     * <p>
     * Any of the fields may be not filterable for every implementation of the {@code ApiJobStore} as the efficiency of
     * filtering is dependent on the backing store. An IllegalArgumentException is thrown if filtering on any given
     * field is not supported.
     *
     * @param jobRowFilters  A Set of JobRowFilters where each JobRowFilter contains the JobField to be
     * filtered on, the filter operation and the values to be compared to.
     *
     * @return An Observable that emits a stream of JobRows that satisfy the given filters
     *
     * @throws IllegalArgumentException if filtering on any field is not supported
     */
    Observable<JobRow> getFilteredRows(Set<JobRowFilter> jobRowFilters) throws IllegalArgumentException;
}
