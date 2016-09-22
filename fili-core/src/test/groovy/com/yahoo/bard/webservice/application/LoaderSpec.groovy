// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application

import static spock.util.matcher.HamcrestMatchers.closeTo
import static spock.util.matcher.HamcrestSupport.expect

import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback

import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

@Timeout(30) // Fail test if hangs
class LoaderSpec extends Specification {

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

    class OneOffTestLoader extends Loader<Boolean> {
        public long start
        public long end
        public static String expectedOneOffName = "OneOffLoader"

        OneOffTestLoader(long delay) {
            super(expectedOneOffName, delay, 0)
        }

        @Override
        void run() {
            start = System.currentTimeMillis()
            sleep(expectedDuration)
            end = System.currentTimeMillis()
        }
    }

    class PeriodicTestLoader extends Loader<Boolean> {
        public long start
        public long end
        public int repeats = expectedRepeats
        public static final String expectedPeriodicName = "PeriodicLoader"

        PeriodicTestLoader(long delay, long period) {
            super(expectedPeriodicName, delay, period)
        }

        @Override
        void run() {
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
        Loader loader = new OneOffTestLoader(expectedDelay)

        expect:
        loader instanceof Loader
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
        Loader loader = new PeriodicTestLoader(expectedDelay, expectedPeriod)

        expect:
        loader instanceof Loader
        loader.getName() == loader.expectedPeriodicName
        loader.getName() == loader.toString()
        loader.getDefinedDelay() == expectedDelay
        loader.getDefinedPeriod() == expectedPeriod
        loader.isPeriodic()
        loader.getErrorCallback() instanceof HttpErrorCallback
        loader.getFailureCallback() instanceof FailureCallback
    }

    def "Test scheduling of a one-off loader"() {
        setup: "Instantiate a task scheduler"
        TaskScheduler scheduler = new TaskScheduler(2)
        Loader<?>loader = new OneOffTestLoader(expectedDelay)

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

    def "Test scheduling of a periodic loader at fixed rate"() {
        setup: "Instantiate a task scheduler"
        TaskScheduler scheduler = new TaskScheduler(2)
        Loader<?> loader = new PeriodicTestLoader(expectedDelay, expectedPeriod)

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
        Loader<?> loader = new PeriodicTestLoader(expectedDelay, expectedPeriod)

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
}
