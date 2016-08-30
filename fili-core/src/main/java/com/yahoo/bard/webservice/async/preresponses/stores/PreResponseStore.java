// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.preresponses.stores;

import com.yahoo.bard.webservice.web.PreResponse;

import rx.Observable;

/**
 * PreResponseStore is responsible for storing PreResponses. It is essentially a key-value mapping between a ticket
 * and the PreResponse associated with that ticket.
 */
public interface PreResponseStore {

    /**
     * Returns an Observable over a PreResponse associated with a given ticket. The Observable's payload contains the
     * PreResponse if the given ticket is present in the PreResponseStore otherwise it's just an empty Observable.
     * <p>
     * In case of any error while connecting to the PreResponseStore, the Observable's
     * {@code onError} method gets called with the exception as payload.
     *
     * @param ticket  The ticket used to identify a job and it's results
     *
     * @return An Observable over a PreResponse associated with the given ticket
     */
    Observable<PreResponse> get(String ticket);

    /**
     * Saves the specified PreResponse in the store. If PreResponse for the given ticket already exists, then that
     * PreResponse will be overwritten. Otherwise, a new ticket-PreResponse mapping is added to the store.
     * <p>
     * This method returns an Observable with the given ticket as a payload. In case of any error while
     * connecting to the PreResponseStore, the Observable's {@code onError} method gets called with the exception as
     * payload.
     *
     * @param ticket  The ticket used to identify a job and it's results
     * @param preResponse  The PreResponse associated with the given ticket
     *
     * @return An Observable with the ticket as a payload or an exception in case of an error
     */
    Observable<String> save(String ticket, PreResponse preResponse);
}
