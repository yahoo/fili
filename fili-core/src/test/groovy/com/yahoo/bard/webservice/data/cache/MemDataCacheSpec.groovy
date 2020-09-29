// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache


import static com.yahoo.bard.webservice.data.cache.MemDataCache.SYSTEM_CONFIG

import net.spy.memcached.MemcachedClient
import net.spy.memcached.internal.OperationFuture
import spock.lang.Specification

class MemDataCacheSpec extends Specification {
    MemDataCache memDataCache;
    MemcachedClient client;

    void setup() {
        SYSTEM_CONFIG.setProperty(MemDataCache.OPERATION_TIMEOUT_CONFIG_KEY, "12345")
        SYSTEM_CONFIG.setProperty(MemDataCache.EXPIRATION_KEY, "1234")
    }

    void cleanup() {
        SYSTEM_CONFIG.clearProperty(MemDataCache.OPERATION_TIMEOUT_CONFIG_KEY)
        SYSTEM_CONFIG.clearProperty(MemDataCache.EXPIRATION_KEY)
    }

    def "Constructor has correct expiration and timeout"() {
        given: // will throw errors because no remote socket is open
        memDataCache = new MemDataCache();

        expect:
        memDataCache.client.operationTimeout == 12345
    }

    def "Set uses the configured expiration and doesn't block anymore"() {
        given:
        OperationFuture future = Mock(OperationFuture)
        MemcachedClient client = Mock(MemcachedClient)
        memDataCache = new MemDataCache(client);

        when:
        boolean result = memDataCache.set("key", "value")

        then:
        1 * client.set("key", 1234, "value") >> future
        0 * future.get()
        result
    }
}
