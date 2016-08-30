// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.broadcastchannels;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import org.redisson.RedissonClient;
import org.redisson.core.RTopic;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.inject.Singleton;

/**
 * A Redis backed implementation of BroadcastChannel. Redisson is used as the Redis-Client. RedisBroadcastChannel has a
 * 'topic' that publishes to the REDIS_CHANNEL. It also has a listener that listens to all the messages on
 * REDIS_CHANNEL and populates an observable. BroadcastListeners can subscribe to this observable to get messages
 * passed around on REDIS_CHANNEL.
 *
 * @param <T>  The type of message that the RedisBroadcastChannel publishes
 */
@Singleton
public class RedisBroadcastChannel<T> implements BroadcastChannel<T> {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final String REDIS_CHANNEL = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("redisbroadcastchannel_name"),
            "preResponse_notification_channel"
    );

    private volatile RTopic<T> topic;
    private final Subject<T, T> notifications;
    private final int listenerId;
    private final ReadWriteLock topicReadWriteLock;

    /**
     * Builds a Broadcastchannel that knows how to communicate with Redis.
     *
     * @param redissonClient  The client to use to communicate with Redis
     */
    public RedisBroadcastChannel(RedissonClient redissonClient) {
        this.topic = redissonClient.getTopic(REDIS_CHANNEL);
        this.notifications = PublishSubject.create();
        this.topicReadWriteLock = new ReentrantReadWriteLock();
        listenerId = topic.addListener((channel, msg) -> { notifications.onNext(msg); });
    }

    @Override
    public void publish(T message) throws UnsupportedOperationException {
        topicReadWriteLock.readLock().lock();
        try {
            if (topic == null) {
                throw new UnsupportedOperationException(PUBLISH_ON_CLOSED_ERROR_MESSAGE);
            }
            topic.publish(message);
        } finally {
            topicReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public Observable<T> getNotifications() {
        return notifications;
    }

    @Override
    public void close() {
        topicReadWriteLock.writeLock().lock();
        try {
            if (topic != null) {
                topic.removeListener(listenerId);
                topic = null;
                notifications.onCompleted();
            }
        } finally {
            topicReadWriteLock.writeLock().unlock();
        }
    }
}
