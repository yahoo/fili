// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application

import static spock.util.matcher.HamcrestMatchers.closeTo
import static spock.util.matcher.HamcrestSupport.expect

import com.yahoo.bard.webservice.data.dimension.TimeoutException
import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.logging.TestLogAppender

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

@Timeout(30) // Fail test if hangs
class LoadTaskSpec extends Specification {

    TestLogAppender logAppender = new TestLogAppender()

    // All times are in millis unless specified otherwise
    int expectedDelay = 500
    int expectedPeriod = 500
    // In general the expected duration shouldn't be higher than the period.
    int expectedDuration = 100
    int expectedRepeats = 4
    int waitingTime = 200
    // The choice of checkingFrequency affects the choice of epsilon. It needs to be epsilon >= checkingFrequency
    int checkingFrequency = 20
    int epsilon = 50
    long delay
    long doneTime
    long startTime

    class OneOffTestLoadTask extends LoadTask<Boolean> {
        public long start
        public long end
        public static String expectedOneOffName = "OneOffLoader"

        OneOffTestLoadTask(long delay) {
            super(expectedOneOffName, delay, 0)
        }

        @Override
        void runInner() {
            start = System.currentTimeMillis()
            sleep(expectedDuration)
            end = System.currentTimeMillis()
        }
    }

    class PeriodicTestLoadTask extends LoadTask<Boolean> {
        public long start
        public long end
        public int repeats = expectedRepeats
        public static final String expectedPeriodicName = "PeriodicLoader"

        PeriodicTestLoadTask(long delay, long period) {
            super(expectedPeriodicName, delay, period)
        }

        @Override
        void runInner() {
            start = System.currentTimeMillis()
            sleep(expectedDuration)
            end = System.currentTimeMillis()
            if (--repeats <= 0) {
                // Needs to cancel itself because runAndReset resets the task status after every run to NEW.
                this.getFuture().
                        orElseThrow({ new IllegalStateException("Failed to schedule loader in a timely manner.") }).
                        cancel(true)
            }
        }
    }

    def "Test constructor of a one-off loader"() {
        setup:
        LoadTask loader = new OneOffTestLoadTask(expectedDelay)

        expect:
        loader instanceof LoadTask
        loader.getName() == loader.expectedOneOffName
        loader.getName() == loader.toString()
        loader.getDefinedDelay() == expectedDelay
        loader.getDefinedPeriod() == 0
        !loader.isPeriodic()
        loader.getErrorCallback() instanceof HttpErrorCallback
        loader.getFailureCallback() instanceof FailureCallback
    }

    def "Test constructor of a periodic loader"() {
        setup:
        LoadTask loader = new PeriodicTestLoadTask(expectedDelay, expectedPeriod)

        expect:
        loader instanceof LoadTask
        loader.getName() == loader.expectedPeriodicName
        loader.getName() == loader.toString()
        loader.getDefinedDelay() == expectedDelay
        loader.getDefinedPeriod() == expectedPeriod
        loader.isPeriodic()
        loader.getErrorCallback() instanceof HttpErrorCallback
        loader.getFailureCallback() instanceof FailureCallback
    }

    @Ignore("Test is flaky in slower environments")
    def "Test scheduling of a one-off loader"() {
        setup: "Instantiate a task scheduler"
        TaskScheduler scheduler = new TaskScheduler(2)
        LoadTask<?> loader = new OneOffTestLoadTask(expectedDelay)

        when: "The loader is scheduled to run"
        // doneTime - startTime is expected slightly higher then the actual duration, mainly because of the
        // checkingFrequency. Choosing an appropriately low epsilon should account for this small extra time.
        startTime = System.currentTimeMillis()
        loader.setFuture(scheduler.schedule(loader, loader.getDefinedDelay(), TimeUnit.MILLISECONDS))

        sleep(waitingTime)

        ScheduledFuture<?> future = loader.getFuture().
                orElseThrow({ new IllegalStateException("Failed to schedule loader.") })

        delay = future.getDelay(TimeUnit.MILLISECONDS)

        synchronized (this) {
            while (!future.isDone()) {
                this.wait(checkingFrequency)
            }
        }
        doneTime = System.currentTimeMillis()

        then: "The loader runs within the expected boundaries"

        expect delay, closeTo(expectedDelay - waitingTime, epsilon)
        // The timeline is as follows:
        // |---delay--|--task duration--|
        expect doneTime - startTime, closeTo(loader.end - loader.start + expectedDelay, epsilon)

        cleanup:
        scheduler.shutdownNow()
    }

    @Ignore("Test is flaky in slower environments")
    def "Test scheduling of a periodic loader at fixed rate"() {
        setup: "Instantiate a task scheduler"
        TaskScheduler scheduler = new TaskScheduler(2)
        LoadTask<?> loader = new PeriodicTestLoadTask(expectedDelay, expectedPeriod)

        when: "The loader is scheduled to run periodically"
        // doneTime - startTime is expected slightly higher then the actual duration, mainly because of the
        // checkingFrequency. Choosing an appropriately low epsilon should account for this small extra time.
        startTime = System.currentTimeMillis()
        loader.setFuture(
                scheduler.scheduleAtFixedRate(
                        loader,
                        loader.getDefinedDelay(),
                        loader.getDefinedPeriod(),
                        TimeUnit.MILLISECONDS
                )
        )

        sleep(waitingTime)

        ScheduledFuture<?> future = loader.getFuture().
                orElseThrow({ new IllegalStateException("Failed to schedule loader.") })

        delay = future.getDelay(TimeUnit.MILLISECONDS)
        synchronized (this) {
            while (!future.isDone()) {
                this.wait(checkingFrequency)
            }
        }
        doneTime = System.currentTimeMillis()

        then: "The loader runs within the expected boundaries"

        expect delay, closeTo(expectedDelay - waitingTime, epsilon)
        // The timeline is as follows:
        // |--delay--|--period--|--period--| ... |--period--|--task's duration--|
        expect doneTime - startTime, closeTo(
                expectedDelay + expectedPeriod * (expectedRepeats - 1) + (loader.end - loader.start),
                epsilon
        )

        cleanup:
        scheduler.shutdownNow()
    }

    @Ignore("It has been difficult to find a reasonable epsilon that won't frequently fail on slow build servers")
    def "Test scheduling of a periodic loader with fixed delay"() {
        setup: "Instantiate a task scheduler"
        TaskScheduler scheduler = new TaskScheduler(2)
        LoadTask<?> loader = new PeriodicTestLoadTask(expectedDelay, expectedPeriod)

        when: "The loader is scheduled to run periodically"
        // doneTime - startTime is expected slightly higher then the actual duration, mainly because of the
        // checkingFrequency. Choosing an appropriately low epsilon should account for this small extra time.
        startTime = System.currentTimeMillis()
        loader.setFuture(
                scheduler.scheduleWithFixedDelay(
                        loader,
                        loader.getDefinedDelay(),
                        loader.getDefinedPeriod(),
                        TimeUnit.MILLISECONDS
                )
        )

        sleep(waitingTime)

        ScheduledFuture<?> future = loader.getFuture().
                orElseThrow({ new IllegalStateException("Failed to schedule loader.") })

        delay = future.getDelay(TimeUnit.MILLISECONDS)
        synchronized (this) {
            while (!future.isDone()) {
                this.wait(checkingFrequency)
            }
        }
        doneTime = System.currentTimeMillis()

        then: "The loader runs within the expected boundaries"

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

    class FailingTestLoadTask extends LoadTaskSpec.OneOffTestLoadTask {


        public static final String UNIQUE_VALUE_1 = "UNIQUE_VALUE_1"

        FailingTestLoadTask(long delay) {
            super(delay)
        }

        @Override
        void runInner() {
            throw new TimeoutException(UNIQUE_VALUE_1);
        }
    }


    def "Test error gets logged on failed test"() {
        setup:
        setup: "Instantiate a task scheduler"
        ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()


        TaskScheduler scheduler = new TaskScheduler(2)
        LoadTask<Boolean> loader = new FailingTestLoadTask(expectedDelay)
        loader.setFuture(
                scheduler.schedule(
                        loader,
                        1,
                        TimeUnit.MILLISECONDS
                )
        )
        Thread.sleep(10)

        expect:
        loader.failed
        logAppender.getMessages().find { it.contains(/UNIQUE_VALUE_1/) }
    }
}
