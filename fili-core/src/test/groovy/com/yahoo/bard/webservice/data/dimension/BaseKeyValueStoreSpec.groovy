// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension

import spock.lang.Specification
import spock.lang.Timeout

@Timeout(30)
abstract class BaseKeyValueStoreSpec extends Specification {

    abstract KeyValueStore getInstance(String storeName)

    abstract void removeInstance(String storeName)

    /** A common store used by most tests.
     */
    KeyValueStore store1

    def setup() {
        store1 = getInstance("test_store1")
        assert store1.isOpen()
    }

    def cleanup() {
        store1.close()
        removeInstance("test_store1")
    }

    def "get a nonexistent key returns null"() {
        given: 'the key does not exist in the store'
        store1.remove("key1")

        expect: 'getting the key returns null'
        null == store1.get("key1")
    }

    def "put a key"() {
        given: 'the key does not exist'
        store1.remove("key1")

        when: 'putting a key with a value'
        store1.put("key1", "value1")

        then: 'the value was stored'
        "value1" == store1.get("key1")
    }

    def "put a key with a null value removes the key"() {
        given: 'the key exists'
        store1.put("key1", "value")

        when: 'putting a key with a null value'
        store1.put("key1", null)

        then: 'the key does not exist'
        null == store1.get("key1")
    }

    def "put a nonexistent key with a null value doesn't do anything"() {
        given: 'the key does not exist'
        store1.remove("key1")

        when: 'putting a key with a null value'
        store1.put("key1", null)

        then: 'the key does not exist'
        null == store1.get("key1")
    }

    def "get/put in different stores are independent"() {
        setup:
        KeyValueStore store2 = getInstance("test_store2")

        when: 'putting the same key into different stores'
        store1.put("key1", "value1")
        store2.put("key1", "value2")

        then: 'the stores are independent'
        "value1" == store1.get("key1")
        "value2" == store2.get("key1")

        cleanup:
        store2.close()
        removeInstance("test_store2")
    }

    def "get/put multiple keys"() {
        when: 'putting different keys into the same store'
        store1.put("key1", "value1")
        store1.put("key2", "value2")

        then: 'the correct values are stored'
        "value1" == store1.get("key1")
        "value2" == store1.get("key2")
    }

    def "remove returns previous value"() {
        given: 'the key exists'
        store1.put("key1", "value1")

        expect: 'removing the key returns the previous value'
        "value1" == store1.remove("key1")
        null == store1.get("key1")
    }

    def "put returns previous value"() {
        when: 'the key exists'
        store1.put("key1", "value1")

        then: 'overwriting the key returns the previous value'
        "value1" == store1.put("key1", "value2")
        "value2" == store1.get("key1")

        when: 'the key does not exist'
        store1.remove("key1")

        then: 'overwriting the key returns null'
        null == store1.put("key1", "value1")
        "value1" == store1.get("key1")
    }

    def "putAll puts all keys"() {
        Map<String, String> entries = [
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
        ]

        when: 'putting multiple keys'
        store1.putAll(entries)

        then: 'all values are stored'
        "value1" == store1.get("key1")
        "value2" == store1.get("key2")
        "value3" == store1.get("key3")
    }

    def "putAll removes keys with null values"() {
        Map<String, String> entries = [
            "key1": "value1",
            "key2": null,
            "key3": "value3",
        ]

        store1.put("key1", "oldValue1")
        store1.put("key2", "oldValue2")
        store1.put("key3", "oldValue3")

        when: 'putting multiple keys'
        store1.putAll(entries)

        then: 'all values are stored/removed'
        "value1" == store1.get("key1")
        null == store1.get("key2")
        "value3" == store1.get("key3")
    }

    def "putAll returns all previous values"() {
        Map<String, String> entries = [
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
        ]

        store1.put("key1", "oldValue1")
        store1.remove("key2")
        store1.put("key3", "oldValue3")

        when: 'putting multiple keys'
        Map previousValues = store1.putAll(entries)

        then: 'the previous values are returned'
        "oldValue1" == previousValues.get("key1")
        null == previousValues.get("key2")
        "oldValue3" == previousValues.get("key3")
    }
}
