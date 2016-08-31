// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.stores;

import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;

import rx.Observable;

import java.util.Set;

import javax.inject.Singleton;

/**
 * An ApiJobStore that doesn't actually do anything. Used as a default binding implementation for the
 * ApiJobStore, allowing users who have no interest in asynchronous requests to set up a Bard instance without
 * having to setup any asynchronous infrastructure.
 */
@Singleton
public class NoOpApiJobStore implements ApiJobStore {

    @Override
    public Observable<JobRow> get(String id) {
        return Observable.empty();
    }

    @Override
    public Observable<JobRow> save(JobRow metadata) {
        return Observable.empty();
    }

    @Override
    public Observable<JobRow> getAllRows() {
        return Observable.empty();
    }

    /**
     * This method ignores the filters and returns an empty Observable.
     *
     * @param jobRowFilter  A Set of JobRowFilters where each JobRowFilter contains the JobField to be
     * filtered on, the filter operation and the values to be compared to.
     *
     * @return  An empty Observable.
     */
    @Override
    public Observable<JobRow> getFilteredRows(Set<JobRowFilter> jobRowFilter) {
        return Observable.empty();
    }
}
