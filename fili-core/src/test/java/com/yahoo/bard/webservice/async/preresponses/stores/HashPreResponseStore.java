// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.async.preresponses.stores;

import com.yahoo.bard.webservice.web.PreResponse;

import rx.Observable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * An in-memory implementation of PreResponseStore mainly for testing purposes. It only provides functionality to save
 * an entry to store and get an entry from the store. It does not have delete functionality nor does it take care of
 * cleaning stale data.
 * <p>
 * Since the HashPreResponseStore is intended primarily for testing, it also includes two maps of tickets to
 * countdown latches, one for getting results, and one for saving results. Just after a ticket has been successfully
 * extracted/saved from/to the store, the appropriate latch is decremented if one exists. This allows tests to know
 * when a certain operation has completed before moving on in the test.
 */
public class HashPreResponseStore implements PreResponseStore {

    private final Map<String, PreResponse> preResponseStore = new LinkedHashMap<>();

    private final Map<String, CountDownLatch> getLatches = new LinkedHashMap<>();
    private final Map<String, CountDownLatch> saveLatches = new LinkedHashMap<>();

    /**
     * Adds a countdown latch for tracking requesting a particular ticket.
     *
     * @param ticket  The ticket whose accesses should be counted
     * @param latch  The latch to be decremented whenever the specified ticket is requested
     */
    public void addGetLatch(String ticket, CountDownLatch latch) {
        getLatches.put(ticket, latch);
    }

    /**
     * Clears all get latches.
     */
    public void clearGetLatches() {
        getLatches.clear();
    }

    /**
     * Adds a countdown latch for tracking saving a particular ticket.
     *
     * @param ticket  The ticket whose saves should be counted
     * @param latch  The latch to be decremented whenever the specified ticket is saved
     */
    public void addSaveLatch(String ticket, CountDownLatch latch) {
        saveLatches.put(ticket, latch);
    }

    /**
     * Clears all save latches.
     */
    public void clearSaveLatches() {
        saveLatches.clear();
    }

    @Override
    public Observable<PreResponse> get(String ticket) {
        PreResponse preResponse = preResponseStore.get(ticket);
        if (getLatches.containsKey(ticket)) {
            getLatches.get(ticket).countDown();
        }
        return preResponse != null ? Observable.just(preResponse) : Observable.empty();
    }

    @Override
    public Observable<String> save(String ticket, PreResponse preResponse) {
        preResponseStore.put(ticket, preResponse);
        if (saveLatches.containsKey(ticket)) {
            saveLatches.get(ticket).countDown();
        }
        return Observable.just(ticket);
    }
}
