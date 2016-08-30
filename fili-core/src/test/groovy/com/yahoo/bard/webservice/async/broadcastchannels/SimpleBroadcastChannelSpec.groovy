// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.broadcastchannels

import rx.subjects.PublishSubject

class SimpleBroadcastChannelSpec extends BroadcastChannelSpec {

    private final PublishSubject<String> crossBoxObservable = PublishSubject.create()

    @Override
    BroadcastChannel getBroadcastChannel() {
        return new SimpleBroadcastChannel<String>(crossBoxObservable)
    }
}
