// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.preresponses.stores;

import com.yahoo.bard.webservice.web.PreResponse;

import rx.Observable;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An in-memory implementation of PreResponseStore mainly for testing purposes. It only provides functionality to save
 * an entry to store and get an entry from the store. It does not have delete functionality nor does it take care of
 * cleaning stale data.
 */
public class HashPreResponseStore implements PreResponseStore {

    private final Map<String, PreResponse> preResponseStore;

    /**
     * Initialize the preResponseStore.
     */
    public HashPreResponseStore() {
        preResponseStore = new LinkedHashMap<>();
    }

    @Override
    public Observable<PreResponse> get(String ticket) {
        PreResponse preResponse = preResponseStore.get(ticket);
        return preResponse != null ? Observable.just(preResponse) : Observable.empty();
    }

    @Override
    public Observable<String> save(String ticket, PreResponse preResponse) {
        preResponseStore.put(ticket, preResponse);
        return Observable.just(ticket);
    }
}
