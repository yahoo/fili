// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.broadcastchannels

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider

import org.redisson.Config;
import org.redisson.Redisson
import org.redisson.RedissonClient
import org.redisson.core.MessageListener
import org.redisson.core.RTopic

class RedisBroadcastChannelSpec extends BroadcastChannelSpec {


    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.instance

    boolean USE_REAL_REDIS_CLIENT = SYSTEM_CONFIG.getBooleanProperty(
            SYSTEM_CONFIG.getPackageVariableName("use_real_redis_client"),
            false
    );

    String REDIS_CHANNEL = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("redisbroadcastchannel_name"),
            "preResponse_notification_channel"
    );

    List<RedissonClient> redissonClients = new ArrayList<>()

    List<MessageListener<String>> topicListeners = new ArrayList<>()

    @Override
    public BroadcastChannel<String> getBroadcastChannel() {
        RedissonClient redissonClient = USE_REAL_REDIS_CLIENT ? Redisson.create(createConfig()) : mockRedissonClient()
        redissonClients << redissonClient
        return new RedisBroadcastChannel<String>(redissonClient)
    }

    public RedissonClient mockRedissonClient() {
        RedissonClient redissonClient = Mock(RedissonClient)
        RTopic<String> topic = Mock(RTopic)

        redissonClient.getTopic(REDIS_CHANNEL) >> topic
        topic.addListener(_) >> { MessageListener<String> messageListener ->
            topicListeners.add(messageListener)
            return topicListeners.size()-1
        }

        topic.publish(_) >> { String message ->
            topicListeners.each { it.onMessage( REDIS_CHANNEL, message)}
            return topicListeners.size()
        }

        return redissonClient
    }

    public static Config createConfig() {
        // Redis server details
        String redisHost = SYSTEM_CONFIG.getStringProperty(
                SYSTEM_CONFIG.getPackageVariableName("redis_host"),
                "localhost"
        )

        int redisPort = SYSTEM_CONFIG.getIntProperty(
                SYSTEM_CONFIG.getPackageVariableName("redis_port"),
                6379
        )

        Config config = new Config()
        config.useSingleServer().setAddress("$redisHost:$redisPort")
        return config
    }

    @Override
    def childCleanup() {
        //shutdown Redisson clients
        redissonClients.each { it.shutdown() }
    }
}
