// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.stores

import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.JOB_TICKET
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.QUERY

import com.yahoo.bard.webservice.async.jobs.JobTestUtils
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow
import com.yahoo.bard.webservice.util.ReactiveTestUtils

import rx.observers.TestSubscriber

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Provides a test suite that defines what it means to implement the ApiJobStore. If an implementation does not
 * pass these tests, then it does not satisfy the ApiJobStore contract.
 * <p>
 * To use these tests, extend this specification and override {@link ApiJobStoreSpec#getStore}. The suite
 * also provides childSetup, childCleanup, childSetupSpec and childCleanupSpec to allow stores to perform
 * their own custom setup and cleanup. The child setup methods are run after the parent methods, while the
 * child cleanup methods are run before the parent methods. This is the same ordering as that used internally by
 * Spock when both a child and parent specification have setup/cleanup methods.
 */
abstract class ApiJobStoreSpec extends Specification {

    ApiJobStore store

    /**
     * Returns an instance of the {@link ApiJobStore} implementation that is under test.
     *
     * @return An empty instance of the {@link ApiJobStore} under test
     */
    abstract ApiJobStore getStore()

    /**
     * A map of test data from numeric ids (as Strings) to JobRows used in the subsequent tests.
     */
    static final Map<String, JobRow> ROW_DATA = [
            "1": JobTestUtils.buildJobRow(1),
            "2": JobTestUtils.buildJobRow(2),
            "3": JobTestUtils.buildJobRow(3),
    ]

    /**
     * Returns the set of all JobRows stored in the ApiJobStore returned by {@link ApiJobStoreSpec#getStore}. This
     * should be if and only if a child specification adds additional data to the store returned by
     * {@link ApiJobStoreSpec#getStore}.
     *
     * @return A set of all the JobRows stored in the JobStore returned by {@linkApiJobStore#getStore}.
     */
    Set<JobRow> getAllRowData() {
        return ROW_DATA.values() as Set<JobRow>
    }

    def childSetup() {}

    def childCleanup() {}

    def childSetupSpec() {}

    def childCleanupSpec() {}

    def setupSpec() {
        childSetupSpec()
    }

    def cleanupSpec() {
        childCleanupSpec()
    }

    def setup() {
        store = getStore()
        // Can't use each because of weirdness in Spock not allowing us to update the store field
        // Try running ROW_DATA.values().each { store.set(value.getId(), value).subscribe() } and watch store not get
        // populated!
        for (JobRow value : ROW_DATA.values()) {
            /*
             * The `ApiJobStore` is intended to support non-blocking implementations of `save`. Therefore, we need to
             * force invocations of `save` to be blocking in test setup. Otherwise, we will find ourselves in a race
             * condition where a test may be executed before the store has finished storing the initial test data.
             *
             * `toBlocking()` transforms the `Observable` returned by `save` into a blocking `Observable`, while
             * invoking `first` forces the blocking `Observable` to block, and return the first message as soon as it
             * is available.
             */
            store.save(value).toBlocking().first()
        }
        childSetup()
    }

    def cleanup() {
        childCleanup()
    }

    @Unroll
    def "Getting #id returns #oldMetadata, but after setting #id to #newMetadata, getting #id returns #newMetadata"() {
        given: "A subscriber for getting, one for setting, and one for getting the updated value"
        TestSubscriber<JobRow> getSubscriber = new TestSubscriber<>()
        TestSubscriber<JobRow> updateSubscriber = new TestSubscriber<>()
        TestSubscriber<JobRow> setSubscriber = new TestSubscriber<>()

        when: "We get the data at id"
        store.get(id).subscribe(getSubscriber)

        then: "We get back the data originally stored there"
        ReactiveTestUtils.assertCompletedWithoutError(getSubscriber)
        getSubscriber.assertReceivedOnNext(oldMetadata)

        when: "We save the new data to the store"
        store.save(newMetadata).subscribe(setSubscriber)

        then: "We get back the id of the data stored"
        ReactiveTestUtils.assertCompletedWithoutError(setSubscriber)
        setSubscriber.assertReceivedOnNext([newMetadata])

        when: "We attempt to get that same id"
        store.get(id).subscribe(updateSubscriber)

        then: "We get the updated data"
        ReactiveTestUtils.assertCompletedWithoutError(updateSubscriber)
        updateSubscriber.assertReceivedOnNext([newMetadata])

        where:
        id  | oldMetadata                | newMetadata
        "0" | []                         | new JobRow(JOB_TICKET, [(JOB_TICKET): "0"])
        "1" | [ROW_DATA["1"]]            | new JobRow(JOB_TICKET, [(JOB_TICKET): "1", (QUERY): "https://host:port/v1/data/table/grain?metrics=11"])
    }

    def "getAllRows returns all the rows stored in the job table"() {
        given: "A subscriber to listen for all the rows"
        TestSubscriber<JobRow> allRowsSubscriber = new TestSubscriber<>()

        when: "We subscribe to observer that gets all the rows"
        store.getAllRows().subscribe(allRowsSubscriber)

        then: "The subscriber gets all the rows"
        ReactiveTestUtils.assertCompletedWithoutError(allRowsSubscriber)
        allRowsSubscriber.getOnNextEvents() as Set == getAllRowData()
    }
}
