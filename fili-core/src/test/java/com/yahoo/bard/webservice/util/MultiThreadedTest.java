// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.ArrayList;
import java.util.List;

/**
 * MultiThreadedTest.TestThread will busy wait for startTime and then run test until stopTime.
 * <p>
 * Choose either Once or Loop type TestThread
 */
public abstract class MultiThreadedTest {

    /**
     * Instantiate and run the Loop test. One thread instance for each available processor
     *
     * @param <T> TestThread subclass type
     * @param cls  TestThread subclass
     * @param nanos  run duration ns
     */
    static <T extends MultiThreadedTest.Loop> void runTestThreadsLoop(Class<T> cls, long nanos) {
        int cpus = Runtime.getRuntime().availableProcessors();
        runTestThreads(cls, cpus, nanos);
    }

    /**
     * Instantiate and run the Once each test with given threadCount.
     *
     * @param <T> TestThread subclass type
     * @param cls  TestThread subclass
     * @param threadCount  number of test threads to run
     */
    static <T extends MultiThreadedTest.Once> void runTestThreadsOnce(Class<T> cls, int threadCount) {
        runTestThreads(cls, threadCount, 300000000000L); // Run for 300 seconds
    }

    /**
     * Runs test threads.
     *
     * @param <T> TestThread subclass type
     * @param cls  TestThread subclass
     * @param threadCount  number of test threads to run
     * @param nanos  run duration ns
     */
    static <T extends MultiThreadedTest.TestThread> void runTestThreads(Class<T> cls, int threadCount, long nanos) {
        // on your mark
        long startTime = System.nanoTime() + 200000000L; // now + 0.2 sec

        List<TestThread> threads = new ArrayList<>();
        for (int threadNumber = 0; threadNumber < threadCount; threadNumber++) {
            MultiThreadedTest.TestThread thread;
            try {
                thread = cls.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            thread.setName("TestThread-" + threadNumber);

            thread.startTime = startTime;
            thread.stopTime = startTime + nanos;
            thread.testId = threadNumber;

            threads.add(thread);
        }

        runTestThreads(threads);
    }

    /**
     * Runs list of test threads.
     *
     * @param threads  List of test threads
     * @param <T>  Type of TestThread this will take a list of and return
     */
    static <T extends MultiThreadedTest.TestThread> void runTestThreads(List<T> threads) {
        // on your mark
        long startTime = System.nanoTime() + 200000000L; // now + 0.2 sec

        // go
        for (T thread : threads) {
            if (thread.startTime == 0) {
                thread.startTime = startTime;
            }
            thread.start();
        }

        // wait
        for (T thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (thread.cause != null) {
                throw new RuntimeException(thread.cause);
            }
        }
    }

    /**
     * Common root test thread.
     */
    public static abstract class TestThread extends Thread {
        protected int testId;      // Test#
        protected long startTime;  // Time to start
        protected long stopTime;   // Time to stop test for Loop

        protected RuntimeException cause = null;

        /**
         * method to run the test.
         */
        protected abstract void runTest();
    }

    /**
     * Run Once per thread test.
     */
    public static abstract class Once extends TestThread {

        @Override
        public void run() {
            try {
                Thread.sleep(1);

                // busy wait so all threads start at same time
                while (System.nanoTime() - startTime < 0) {
                    // No-op
                }

                runTest();
            } catch (Throwable t) {
                // save any exception to be re-thrown by runTestThreads
                cause = new RuntimeException(t);
                t.printStackTrace();
            }
        }
    }

    /**
     * Run Loop test.
     */
    public static abstract class Loop extends TestThread {

        @Override
        public void run() {
            try {
                Thread.sleep(1);

                // busy wait so all threads start at same time
                while (System.nanoTime() - startTime < 0) {
                    // No-op
                }

                // test should run at least once per thread
                runTest();

                // test loop
                while (System.nanoTime() - stopTime < 0) {
                    // run multiple tests per loop to reduce check overhead
                    runTest();
                    runTest();
                    runTest();
                    runTest();
                    runTest();
                }
            } catch (Throwable t) {
                // save any exception to be re-thrown by runTestThreads
                cause = new RuntimeException(t);
            }
        }
    }
}
