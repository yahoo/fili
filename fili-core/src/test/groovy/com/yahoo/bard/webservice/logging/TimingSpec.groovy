// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging

import static spock.util.matcher.HamcrestMatchers.closeTo
import static spock.util.matcher.HamcrestSupport.expect

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import ch.qos.logback.classic.spi.ILoggingEvent
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

/**
 * Tests simple initialization of the outmost timing wrapper and the mega log line
 */
@Timeout(30)    // Fail test if hangs
class TimingSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    @Shared
    String pkgString = "com.yahoo.bard.webservice.logging.blocks."

    @Shared
    TestLogAppender logAppender

    def setupSpec() {
        // Hook with test appender
        logAppender = new TestLogAppender()
        // Reset request log before using it in this test
        RequestLog.dump()
    }

    def cleanupSpec() {
        logAppender.close()
    }

    def setup() {
        logAppender.clear()
    }

    def cleanup() {
        // Cleanup appender after each test
        logAppender.clear()
    }

    Map<String, Number> extractTimesFromLogs(String... timers) {
        Map<String, Number> result = [:]

        // Extract the number of log lines
        result.numLines = 0

        // Find the line that contains "uuid"
        for (ILoggingEvent logEvent : logAppender.getEvents()) {
            String logLine = logEvent.getMessage()

            if (logEvent.getThreadName().contains("[Finalizer]")) {
                continue;
            }
            ++result.numLines
            if (logLine.contains('"uuid"')) {
                // Extract the timers we were told about
                JsonNode json = MAPPER.readValue(logLine, JsonNode.class)
                for(String key : timers) {
                    result[key] = json.findValues(key).get(0).asDouble()
                }

                // Extract the number of threads
                result.numThreads = json.findValues("Threads").get(0).size()

                // Return what we extracted
                return result
            }
        }
    }

    def "Normal start and stop of timer logs one line within the expected duration"() {
        given: "A duration to wait between starting and stopping the timer"
        int expectedDuration = 500

        and: "An error bound for the duration"
        int epsilon = 50

        and: "A timer"
        String timerName = "StartStopTest"

        when: "We start the timer, wait the expected duration, and stop the timer"
        RequestLog.startTiming(timerName)
        sleep((int) expectedDuration)
        RequestLog.stopTiming(timerName)
        RequestLog.log()
        Map res = extractTimesFromLogs(timerName)

        then: "One log line is produced in the expected duration range"
        expect res[timerName], closeTo(expectedDuration, epsilon)
    }

    def "Redundant stop of timer logs one line and a warning"() {
        given: "A timer"
        String timerName = "StartStopTest"

        when: "We start the timer and stop it twice"
        RequestLog.startTiming(timerName)
        RequestLog.stopTiming(timerName)
        RequestLog.stopTiming(timerName)
        RequestLog.log()

        then: "One log line is produced and a warning"
        String warningLine
        boolean didLog = false
        int numLines = logAppender.size()
        for (String logLine : logAppender.getMessages()) {
            if (logLine.contains("\"uuid\"")) {
                didLog = true
            } else {
                warningLine = logLine
            }
        }

        numLines == 2
        didLog == true
        warningLine == "Tried to stop timer that has not been started: " + timerName
    }

    def "Check redundant start on timer"() {
        given: "A timer"
        String timerName = "StartStopTest"

        when: "We start the timer and stop it twice"
        RequestLog.startTiming(timerName)
        RequestLog.startTiming(timerName)
        RequestLog.stopTiming(timerName)
        RequestLog.log()

        then: "One log line is produced and a warning"
        String warningLine
        boolean didLog = false
        int numLines = logAppender.size()
        for (String logLine : logAppender.getMessages()) {
            if (logLine.contains("\"uuid\"")) {
                didLog = true
            } else {
                warningLine = logLine
            }
        }

        numLines == 2
        didLog == true
        warningLine == "Tried to start timer that is already running: " + timerName
    }

    def "Check timer with thread switching "() {
        given: "A duration to wait between starting and stopping the timer"
        int expectedDuration = 500

        and: "An error bound for the duration"
        int epsilon = 50

        and: "A timer"
        String timerName = "ThreadSwitchTest"

        when: "We start the timer in one thread and stop it in the other after waiting for the expected duration"
        RequestLog.startTiming("ThreadSwitchTest")

        final RequestLog ctx = RequestLog.dump()
        def thread = Thread.start {
            sleep(500)
            RequestLog.restore(ctx)
            RequestLog.stopTiming("ThreadSwitchTest")
            RequestLog.log()
        }
        thread.join()

        Map res = extractTimesFromLogs(timerName)

        then: "One log line is produced, with two threads in the expected duration range"
        res.numLines == 1
        res.numThreads == 2
        expect res[timerName], closeTo(expectedDuration, epsilon)
    }

    def "Check start and stop of nested timers"() {
        given: "A duration to wait between starting and stopping the timer"
        int duration = 500
        int expectedOuterDuration = 2000
        int expectedInnerDuration = 1000

        and: "An error bound for the duration"
        int epsilon = 50

        and: "Two timers"
        String outerTimerName = "Outer"
        String innerTimerName = "Inner"

        when: "We nest a timer within another timer in a for loop"
        for (int i = 0; i < 2; ++i) {
            RequestLog.startTiming(outerTimerName)
            sleep(duration)
            RequestLog.startTiming(innerTimerName)
            sleep(duration)
            RequestLog.stopMostRecentTimer()
            RequestLog.stopTiming(outerTimerName)
        }
        RequestLog.log()
        Map res = extractTimesFromLogs(outerTimerName, innerTimerName)

        then: "One log line is produced in the expected duration range"
        res.numLines == 1
        expect res[outerTimerName], closeTo(expectedOuterDuration, epsilon)
        expect res[innerTimerName], closeTo(expectedInnerDuration, epsilon)
    }

    @Unroll
    def "Test parsing order of LogInfo parts in RequestLog for requested order: #inputOrderString"() {
        expect:
        RequestLog.generateLogInfoOrder(inputOrderString) == expectedList

        where:
        expectedList << [[], [], [], [pkgString + "Part1", pkgString + "Part2"], [pkgString + "Part2", pkgString +
                "Part1"], [pkgString + "Part1"]]
        inputOrderString << ["", ",", " ", "Part1,Part2", "Part2,Part1,", ", Part1"]
    }
}
