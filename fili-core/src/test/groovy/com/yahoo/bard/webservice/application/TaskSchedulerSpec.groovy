// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application

import spock.lang.Specification

import java.util.concurrent.ScheduledThreadPoolExecutor

class TaskSchedulerSpec extends Specification {

    int expectedThreadPoolSize = 4

    def "Test constructor"() {
        setup:
        TaskScheduler scheduler = new TaskScheduler(expectedThreadPoolSize);

        expect:
        scheduler instanceof ScheduledThreadPoolExecutor
        scheduler.getCorePoolSize() == expectedThreadPoolSize

        cleanup:
        scheduler.shutdownNow()
    }

    def "Test termination"() {
        setup:
        TaskScheduler scheduler = new TaskScheduler(expectedThreadPoolSize);

        when:
        scheduler.shutdownNow()

        then:
        scheduler.isShutdown()
    }
}
