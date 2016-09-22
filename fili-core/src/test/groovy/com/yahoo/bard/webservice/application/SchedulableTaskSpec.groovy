// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application

import static spock.util.matcher.HamcrestMatchers.closeTo
import static spock.util.matcher.HamcrestSupport.expect

import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.Delayed
import java.util.concurrent.FutureTask
import java.util.concurrent.RunnableScheduledFuture
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

@Timeout(30) // Fail test if hangs
class SchedulableTaskSpec extends Specification {

    // All times are in millis unless specified otherwise
    int expectedDelay = 500
    int expectedPeriod = 500
    // In general the expected duration shouldn't be higher than the period.
    int expectedDuration = 100
    int expectedRepeats = 4
    int waitingTime = 200
    // The choice of checkingFrequency affects the choice of epsilon. It needs to be epsilon >= checkingFrequency
    int checkingFrequency = 20
    int epsilon = ((expectedDuration + expectedDelay ) / 4 )* 3
    long delay
    long doneTime
    long startTime

    RunnableScheduledFuture<?> innerTask = Mock(RunnableScheduledFuture)

    def "Test constructor of schedulable task"() {
        when:
        SchedulableTask<?> task = new SchedulableTask<>(
                { sleep(expectedDuration) },
                null,
                innerTask
        )

        task.isPeriodic()
        task.getDelay(TimeUnit.MILLISECONDS)
        task.compareTo(Mock(Delayed))

        then:
        task instanceof FutureTask
        task instanceof RunnableScheduledFuture
        1 * innerTask.isPeriodic()
        1 * innerTask.getDelay(_)
        1 * innerTask.compareTo(_)
    }

    def "Test scheduling of a one-off task"() {
        setup: "Instantiate a task scheduler"
        TaskScheduler scheduler = new TaskScheduler(2)
        scheduler.setRemoveOnCancelPolicy(true)

        when: "The task is scheduled to run"
        // doneTime - startTime is expected slightly higher then the actual duration, mainly because of the
        // checkingFrequency. Choosing an appropriately low epsilon should account for this small extra time.
        startTime = System.currentTimeMillis()
        ScheduledFuture<?> future = scheduler.schedule(
                { sleep(expectedDuration) },
                expectedDelay,
                TimeUnit.MILLISECONDS
        )

        sleep(waitingTime)

        delay = future.getDelay(TimeUnit.MILLISECONDS)

        synchronized (this) {
            while (!future.isDone()) {
                this.wait(checkingFrequency)
            }
        }
        doneTime = System.currentTimeMillis()

        then: "The task runs within the expected boundaries"
        future instanceof SchedulableTask
        expect delay, closeTo(expectedDelay - waitingTime, epsilon)
        // The timeline is as follows:
        // |---delay--|--task duration--|
        expect doneTime - startTime, closeTo(expectedDuration + expectedDelay, epsilon)

        cleanup:
        scheduler.shutdownNow()
    }

    def "Test scheduling of a periodic task at fixed rate"() {
        setup: "Instantiate a task scheduler"
        TaskScheduler scheduler = new TaskScheduler(2)
        scheduler.setRemoveOnCancelPolicy(true)

        when: "The task is scheduled to run periodically"
        // doneTime - startTime is expected slightly higher then the actual duration, mainly because of the
        // checkingFrequency. Choosing an appropriately low epsilon should account for this small extra time.
        startTime = System.currentTimeMillis()
        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(
                { sleep(expectedDuration) },
                expectedDelay,
                expectedPeriod,
                TimeUnit.MILLISECONDS
        )

        sleep(waitingTime)

        delay = future.getDelay(TimeUnit.MILLISECONDS)
        synchronized (this) {
            this.wait(delay + expectedPeriod * (expectedRepeats - 1))
            future.cancel(true)
            while (!future.isDone()) {
                future.isDone()
                this.wait(checkingFrequency)
            }
        }
        doneTime = System.currentTimeMillis()

        then: "The task runs within the expected boundaries"

        expect delay, closeTo(expectedDelay - waitingTime, epsilon)
        // The timeline is as follows:
        // |-delay-|-period-|-period-| ... |-period-|
        expect doneTime - startTime, closeTo(expectedDelay + expectedPeriod * (expectedRepeats - 1), epsilon)

        cleanup:
        scheduler.shutdownNow()
    }

    // Since in this Spec the Runnable does not hold a reference to its future, and therefore a periodic task can not
    // cancel itself after executing successfully for a given number of times (expectedRepeats) this test is
    // equivalent to the one above, and it is just testing correct scheduling of periodic tasks with
    // scheduleWithFixedDelay
    def "Test scheduling of a periodic task at fixed delay"() {
        setup: "Instantiate a task scheduler"
        TaskScheduler scheduler = new TaskScheduler(2)
        scheduler.setRemoveOnCancelPolicy(true)

        when: "The task is scheduled to run periodically"
        // doneTime - startTime is expected slightly higher then the actual duration, mainly because of the
        // checkingFrequency. Choosing an appropriately low epsilon should account for this small extra time.
        startTime = System.currentTimeMillis()
        ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(
                { sleep(expectedDuration) },
                expectedDelay,
                expectedPeriod,
                TimeUnit.MILLISECONDS
        )

        sleep(waitingTime)

        delay = future.getDelay(TimeUnit.MILLISECONDS)
        synchronized (this) {
            this.wait(delay + expectedPeriod * (expectedRepeats - 1) + expectedDuration * expectedRepeats)
            future.cancel(true)
            while (!future.isDone()) {
                this.wait(checkingFrequency)
            }
        }
        doneTime = System.currentTimeMillis()

        then: "The task runs within the expected boundaries"

        expect delay, closeTo(expectedDelay - waitingTime, epsilon)
        // The timeline is as follows:
        // |--delay--|--period--|--task's duration--| ... |--period--|--task's duration--|
        expect doneTime - startTime, closeTo(
                expectedDelay + expectedDuration * expectedRepeats + expectedPeriod * (expectedRepeats - 1),
                epsilon
        )

        cleanup:
        scheduler.shutdownNow()
    }
}
