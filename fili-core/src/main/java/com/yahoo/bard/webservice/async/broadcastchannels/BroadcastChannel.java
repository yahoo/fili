// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.broadcastchannels;

import rx.Observable;

import java.io.Closeable;

/**
 * Bard may exist in a system where multiple Bard hosts exist, it is possible that a user may submit a query on one box,
 * and then come back later and perform a long poll on a separate box. Therefore, we need a mechanism to notify all the
 * Bard boxes when a PreResponse is stored in the PreResponseStore by any of the Bard boxes.
 * <p>
 * BroadcastChannel is the interface that lets you send messages to and receive messages from the other boxes in the
 * cluster.
 *
 * @param <T>  The type of message that the BroadcastChannel publishes
 */
public interface BroadcastChannel<T> extends Closeable {

    String PUBLISH_ON_CLOSED_ERROR_MESSAGE = "The BroadcastChannel has been closed, and cannot be published to";

    /**
     * Publish is used to publish a message from a Bard box.
     *
     * @param message  The message to be published
     *
     * @throws UnsupportedOperationException if someone attempts to publish to the channel after it has been closed
     */
    void publish(T message) throws UnsupportedOperationException;

    /**
     * This method returns a Hot Observable (Observable which emits events whether someone is listening or not) that
     * emits all the notifications passed to it by Bard instances in the cluster.
     *
     * @return An unbounded stream of notifications from Bard instances
     */
    Observable<T> getNotifications();
}
