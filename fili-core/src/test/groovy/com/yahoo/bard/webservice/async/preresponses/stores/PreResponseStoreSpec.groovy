// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.preresponses.stores

import com.yahoo.bard.webservice.util.ReactiveTestUtils
import com.yahoo.bard.webservice.web.PreResponse

import rx.observers.TestSubscriber
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Test for PreResponseStore. Any implementation of PreResponseStore should pass these test cases.
 * <p>
 * To use these tests, extend this specification and override {@link PreResponseStoreSpec#getStore}. The suite
 * also provides childSetup, childCleanup, childSetupSpec and childCleanupSpec to allow stores to perform
 * their own custom setup and cleanup. The child setup methods are run after the parent methods, while the
 * child cleanup methods are run before the parent methods. This is the same ordering as that used internally by
 * Spock when both a child and parent specification have setup/cleanup methods.
 */
abstract class PreResponseStoreSpec extends Specification {

    PreResponseStore preResponseStore

    /**
     * Returns an instance of the {@link PreResponseStore} implementation that is under test.
     *
     * @return An empty instance of the {@link PreResponseStore} under test
     */
    abstract PreResponseStore getStore()

    def childSetup() {}

    def childCleanup() {}

    def childSetupSpec() {}

    def childCleanupSpec() {}

    def setupSpec() {
        childSetupSpec()
    }

    def cleanup() {
        childCleanup()
    }

    def cleanupSpec() {
        childCleanupSpec()
    }

    def setup() {
        preResponseStore = getStore()

        (0..1).each {
            /*
             * The `ApiJobStore` is intended to support non-blocking implementations of `save`. Therefore, we need to
             * force invocations of `save` to be blocking in test setup. Otherwise, we will find ourselves in a race
             * condition where a test may be executed before the store has finished storing the initial test data.
             *
             * `toBlocking()` transforms the `Observable` returned by `save` into a blocking `Observable`, while
             * invoking `first` forces the blocking `Observable` to block, and return the first message as soon as it
             * is available.
             */
            preResponseStore.save("$it", PreResponseTestingUtils.buildPreResponse("2016-04-2${it}T00:00:00.000-05:00"))
                    .toBlocking().first()
        }

        childSetup()
    }

    @Unroll
    def "Getting #id returns #oldPreResponse, but after setting #id to #newPreResponse, getting #id returns #newPreResponse"() {
        setup:
        TestSubscriber<PreResponse> getSubscriber = new TestSubscriber<>()
        TestSubscriber<String> saveSubscriber = new TestSubscriber<>()
        TestSubscriber<PreResponse> updateSubscriber = new TestSubscriber<>()

        when:
        preResponseStore.get(id).subscribe(getSubscriber)

        then:
        ReactiveTestUtils.assertCompletedWithoutError(getSubscriber)
        getSubscriber.assertReceivedOnNext(oldPreResponse)

        when:
        preResponseStore.save(id, newPreResponse).subscribe(saveSubscriber)

        then:
        ReactiveTestUtils.assertCompletedWithoutError(saveSubscriber)
        saveSubscriber.assertReceivedOnNext([id])

        and: "verify that the save method correctly changed the value in the store"
        preResponseStore.get(id).subscribe(updateSubscriber)
        ReactiveTestUtils.assertCompletedWithoutError(updateSubscriber)
        updateSubscriber.assertReceivedOnNext([newPreResponse])

        where:
        id  | oldPreResponse                                                              | newPreResponse
        "0" | [PreResponseTestingUtils.buildPreResponse("2016-04-20T00:00:00.000-05:00")] | PreResponseTestingUtils.buildPreResponse("2016-04-23T00:00:00.000-05:00")
        "2" | []                                                                          | PreResponseTestingUtils.buildPreResponse("2016-04-22T00:00:00.000-05:00")
    }
}
