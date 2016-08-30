// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.broadcastchannels

import rx.observers.TestSubscriber
import spock.lang.Specification
import spock.lang.Unroll
import spock.util.concurrent.PollingConditions

/**
 * Test for BroadcastChannel. Any implementation of BroadcastChannel should pass these test cases.
 * <p>
 * To use these tests, extend this specification and override {@link BroadcastChannelSpec#getBroadcastChannel}.
 * The suite also provides childSetup, childCleanup,childSetupSpec and childCleanupSpec to allow stores to perform their
 * own custom setup and cleanup. The child setup methods are run after the parent methods, while the child cleanup
 * methods are run before the parent methods. This is the same ordering as that used internally by Spock when both a
 * child and parent specification have setup/cleanup methods.
 */
abstract class BroadcastChannelSpec extends Specification {
    BroadcastChannel<String> broadcastChannelA
    BroadcastChannel<String> broadcastChannelB

    PollingConditions condition

    /**
     * Returns an instance of the {@link BroadcastChannel} implementation that is under test. This method should return
     * a fresh instance of the BroadcastChannel, backed by the same communication system as every other instance
     * returned by this method i.e. all instances returned by this method in the scope of a single test should be linked.
     *
     * @return An instance of the {@link BroadcastChannel} under test
     */
    abstract BroadcastChannel<String> getBroadcastChannel()

    /**
     * Returns an instance of PollingConditions. PollingConditions repeatedly check to see if broadcastListener receive
     * the messages published by BroadcastChannel before the PollingConditions timeout.
     *
     * @return  an instance of PollingConditions
     */
    PollingConditions getPollingConditions() {
        return new PollingConditions(timeout: 10, initialDelay: 0, factor: 1.25)
    }

    def childSetup() {}

    def childCleanup() {}

    def childSetupSpec() {}

    def childCleanupSpec() {}

    def setupSpec() {
        childSetupSpec()
    }

    def cleanup() {
        //unsubscribe listeners
        broadcastChannelA.close()
        broadcastChannelB.close()
        childCleanup()
    }

    def cleanupSpec() {
        childCleanupSpec()
    }

    def setup() {
        broadcastChannelA = getBroadcastChannel()
        broadcastChannelB = getBroadcastChannel()
        condition = getPollingConditions()

        childSetup()
    }

    def "When a message is sent by one BroadcastChannel, it is broadcast to all BroadcastChannels"() {
        setup:
        TestSubscriber<String> broadcastListenerA = new TestSubscriber<>()
        TestSubscriber<String> broadcastListenerB = new TestSubscriber<>()

        broadcastChannelA.getNotifications().subscribe(broadcastListenerA)

        when: "broadcastChannelA publishes a message"
        broadcastChannelA.publish("PreResponseA1")

        then: "broadcastListenerA receives the message. broadcastListenerB - you snooze you lose"
        validateListener(broadcastListenerA, ["PreResponseA1"])
        validateListener(broadcastListenerB, [])

        when: "broadcastListenerB subscribes to broadcastChannelB"
        broadcastChannelB.getNotifications().subscribe(broadcastListenerB)

        and: "broadcastChannelA publishes a message again"
        broadcastChannelA.publish("PreResponseA2")

        then: "broadcastListenerA and broadcastListenerB receive the message"
        validateListener(broadcastListenerA, ["PreResponseA1", "PreResponseA2"])
        validateListener(broadcastListenerB, ["PreResponseA2"])

        when: "broadcastChannelB publishes a message"
        broadcastChannelB.publish("PreResponseB")

        then: "the message is received by all the broadcastListeners"
        validateListener(broadcastListenerA, ["PreResponseA1", "PreResponseA2", "PreResponseB"])
        validateListener(broadcastListenerB, ["PreResponseA2", "PreResponseB"])
    }

    @Unroll
    def "Publishing to a channel that has been closed #numCloses times throws an UnsupportedOperationException"() {
        given: "A closed broadcast channel"
        closeChannel(broadcastChannelA)

        when: "We attempts to publish to the closed channel"
        broadcastChannelA.publish("This shouldn't work.")

        then: "An exception is thrown"
        thrown UnsupportedOperationException

        where:
        numCloses | closeChannel
        1         | {it.close()}
        2         | {it.close(); it.close()}
    }

    /**
     * Tests whether the testSubscriber receives listenerEvents within the timeout specified by condition
     *
     * @param listener  the TestSubscriber listening to the above events
     * @param events  A List of expected listener events
     */
    void validateListener(TestSubscriber<String> listener, List<String> events) {
        condition.eventually {
            listener.getOnNextEvents() == events
        }
    }
}
