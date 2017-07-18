// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.async.preresponses.stores;

import com.yahoo.fili.webservice.web.PreResponse;

import rx.Observable;

import javax.inject.Singleton;

/**
 * A PreResponseStore that doesn't actually do anything. Used as a default binding implementation for the
 * PreResponseStore, allowing users who have no interest in asynchronous requests to set up a Fili instance without
 * having to setup any asynchronous infrastructure.
 */
@Singleton
public class NoOpPreResponseStore implements PreResponseStore {
    @Override
    public Observable<PreResponse> get(String ticket) {
        return Observable.empty();
    }

    @Override
    public Observable<String> save(String ticket, PreResponse preResponse) {
        return Observable.empty();
    }
}
