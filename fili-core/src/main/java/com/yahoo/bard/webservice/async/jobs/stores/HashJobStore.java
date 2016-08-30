// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.stores;

import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;

import rx.Observable;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An ApiJobStore backed by an in-memory map. This is meant as a stub implementation for
 * testing and playing purposes. It is _not_ meant to be used in production. For one, it stores the ticket
 * information in memory, which is not durable. For another, it does not attempt to cleanup sufficiently old jobs,
 * so its memory footprint will grow until the system is rebooted.
 */
public class HashJobStore implements ApiJobStore {

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
}
