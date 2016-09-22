// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension

import com.yahoo.bard.webservice.config.SystemConfigProvider

import spock.lang.Requires

@Requires({ SystemConfigProvider.getInstance().getStringProperty(
        SystemConfigProvider.getInstance().getPackageVariableName("key_value_store_tests"), "memory").contains("redis") })
class RedisStoreSpec extends BaseKeyValueStoreSpec {

    def KeyValueStore getInstance(String storeName) {
        return RedisStoreManager.getInstance(storeName);
    }

    def void removeInstance(String storeName) {
        RedisStoreManager.removeInstance(storeName);
    }
}
