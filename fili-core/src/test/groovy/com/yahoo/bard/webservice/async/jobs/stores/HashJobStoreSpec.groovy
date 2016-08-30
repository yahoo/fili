// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.stores

import com.yahoo.bard.webservice.async.jobs.JobTestUtils
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow
import com.yahoo.bard.webservice.util.ReactiveTestUtils

/**
 * Verifies that the HashJobStore satisfies the ApiJobStore interface. The tests may be found in
 * {@link ApiJobStoreSpec}.
 */
class HashJobStoreSpec extends ApiJobStoreSpec {

    @Override
    ApiJobStore getStore() {
        new HashJobStore()
    }

    def "The backing map is invoked once per store access regardless of the number of observers subscribed"() {
        given: "A mocked out map, and a store that uses it as the backing data store"
        Map<String, JobRow> mockMap = Mock(Map)
        HashJobStore store = new HashJobStore(mockMap)

        when: "We request data from store, and assign a whole mess of observers to it"
        ReactiveTestUtils.subscribeObservers(store.get("0"), 10)

        and: "save data in the store and assign a whole mess of observers to it"
        ReactiveTestUtils.subscribeObservers(store.save(JobTestUtils.buildJobRow(0)), 10)

        and: "request all the rows in the store and assign a whole mess of observers to it"
        ReactiveTestUtils.subscribeObservers(store.getAllRows(), 10)

        then: "The store's operations are accessed only once"
        1 * mockMap.get(_)
        1 * mockMap.put(_, _)
        1 * mockMap.values() >> ([] as Set)

    }
}
