// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.util.MultiThreadedTest
import com.yahoo.bard.webservice.web.DataApiRequestTypeIdentifier
import com.yahoo.bard.webservice.web.endpoints.TestFilterServlet
import com.yahoo.bard.webservice.web.ratelimit.DefaultRateLimiter

import spock.lang.IgnoreIf
import spock.lang.Retry
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.atomic.AtomicInteger

/* Do not test on Jenkins since URL requests are inconsistent */
@Retry
@Timeout(30)    // Fail test if hangs
@IgnoreIf({System.getenv("BUILD_NUMBER") != null})
class RateLimitFilterSpec extends Specification {
    static final int LIMIT_GLOBAL = 20
    static final int LIMIT_PER_USER = 10
    static final int LIMIT_UI = 15

    static SystemConfig systemConfig = SystemConfigProvider.getInstance()
    static JerseyTestBinder jtb

    def static originalGlobalLimit
    def static originalUserLimit
    def static originalUiLimit

    static void setDefaults() {
        originalGlobalLimit = systemConfig.setProperty(DefaultRateLimiter.REQUEST_LIMIT_GLOBAL_KEY, LIMIT_GLOBAL as String)
        originalUserLimit = systemConfig.setProperty(DefaultRateLimiter.REQUEST_LIMIT_PER_USER_KEY, LIMIT_PER_USER as String)
        originalUiLimit = systemConfig.setProperty(DefaultRateLimiter.REQUEST_LIMIT_UI_KEY, LIMIT_UI as String)
    }

    static void clearDefaults() {
        systemConfig.resetProperty(DefaultRateLimiter.REQUEST_LIMIT_GLOBAL_KEY, originalGlobalLimit)
        systemConfig.resetProperty(DefaultRateLimiter.REQUEST_LIMIT_PER_USER_KEY, originalUserLimit)
        systemConfig.resetProperty(DefaultRateLimiter.REQUEST_LIMIT_UI_KEY, originalUiLimit)
    }

    def setupSpec() {
        setDefaults()

        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(TestRateLimitFilter.class,TestFilterServlet.class)
    }

    def setup() {
        // clear counts
        TestMultiAccess.ok.set(0)
        TestMultiAccess.fail.set(0)

        // reset metrics to 0
        TestRateLimitFilter.instance.rateLimiter.with {
            [requestGlobalCounter, usersCounter].each { it.dec(it.count) }
            [requestBypassMeter, requestUiMeter, requestUserMeter, rejectUiMeter, rejectUserMeter].each {
                it.mark(-it.count)
            }
        }
    }

    def cleanupSpec() {
        jtb.tearDown()
        TestRateLimitFilter.instance = null
        clearDefaults()
    }

    // Multi-threaded Access
    final static class TestMultiAccess extends MultiThreadedTest.Once {

        static AtomicInteger ok = new AtomicInteger()
        static AtomicInteger fail = new AtomicInteger()

        final String user
        final Map<String,String> headers

        TestMultiAccess() {
            this("myuser")
        }

        TestMultiAccess(String username) {
            this(username,null)
        }

        TestMultiAccess(String username, Map<String,String> headers) {
            this.user = username
            this.headers = headers
        }


        void runTest() {

            URL url = new URL("http://localhost:${jtb.getHarness().getPort()}/test/data?"+TestRateLimitFilter.USER_PARAM+"="+this.user+"&t="+Thread.currentThread().getName())

            HttpURLConnection con = url.openConnection()
            con.setInstanceFollowRedirects(false)
            if (headers != null) {
                headers.each{ k, v -> con.setRequestProperty(k, v) }
            }

            switch (con.getResponseCode()) {
                case 200:
                    ok.incrementAndGet()
                    break

                case 429:
                    fail.incrementAndGet()
                    break

                default:
                    fail
            }
        }
    }

    def "Properties read correctly"() {
        expect:
        TestRateLimitFilter.instance.rateLimiter.requestLimitGlobal == LIMIT_GLOBAL
        TestRateLimitFilter.instance.rateLimiter.requestLimitPerUser == LIMIT_PER_USER
        TestRateLimitFilter.instance.rateLimiter.requestLimitUi == LIMIT_UI
    }

    def "User limit reached"() {
        when: "user opens 12 simultaneous requests"
        MultiThreadedTest.runTestThreadsOnce(TestMultiAccess.class, 12)

        then: "2 requests return failure 429"
        TestRateLimitFilter.instance.rateLimiter.globalCount.get() == 0
        TestRateLimitFilter.instance.rateLimiter.requestGlobalCounter.count == LIMIT_PER_USER
        TestRateLimitFilter.instance.rateLimiter.requestBypassMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.requestUiMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.requestUserMeter.count == LIMIT_PER_USER
        TestRateLimitFilter.instance.rateLimiter.rejectUiMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.rejectUserMeter.count == 12 - LIMIT_PER_USER

        TestMultiAccess.ok.get() + TestMultiAccess.fail.get() == 12
        TestMultiAccess.ok.get() == LIMIT_PER_USER
        TestMultiAccess.fail.get() == 12 - LIMIT_PER_USER
        TestRateLimitFilter.instance.rateLimiter.globalCount.get() == 0
    }

    def "Global limit successful"() {

        when: "10 users each open 2 simultaneous requests"
        List<Thread> threads = []
        threads = (1..20).collect {
            new TestMultiAccess("user"+it%10)
        }
        MultiThreadedTest.runTestThreads(threads)

        then: "all are successful"
        TestRateLimitFilter.instance.rateLimiter.globalCount.get() == 0
        TestRateLimitFilter.instance.rateLimiter.requestGlobalCounter.count == 20
        TestRateLimitFilter.instance.rateLimiter.requestBypassMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.requestUiMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.requestUserMeter.count == 20
        TestRateLimitFilter.instance.rateLimiter.rejectUiMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.rejectUserMeter.count == 0

        TestMultiAccess.fail.get() == 0
        TestMultiAccess.ok.get() == 20
    }

    def "Global limit reached"() {
        when: "10 users each open 3 simultaneous requests"
        List<Thread> threads = []
        threads = (1..30).collect {
            new TestMultiAccess("user"+it%10)
        }
        MultiThreadedTest.runTestThreads(threads)

        then: "20 are successful"
        TestRateLimitFilter.instance.rateLimiter.globalCount.get() == 0
        TestRateLimitFilter.instance.rateLimiter.requestGlobalCounter.count == LIMIT_GLOBAL
        TestRateLimitFilter.instance.rateLimiter.requestBypassMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.requestUiMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.requestUserMeter.count == LIMIT_GLOBAL
        TestRateLimitFilter.instance.rateLimiter.rejectUiMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.rejectUserMeter.count == 30 - LIMIT_GLOBAL

        threads.size() == 30
        TestRateLimitFilter.instance.rateLimiter.globalCount.get() == 0
        TestMultiAccess.ok.get() == LIMIT_GLOBAL
        TestMultiAccess.fail.get() == 30 - LIMIT_GLOBAL
    }

    def "Limit ignored when bypass header found"() {
        when: "user configures bypass header and opens 30 simultaneous requests"
        Map headers = [ (DataApiRequestTypeIdentifier.BYPASS_HEADER_NAME) : (DataApiRequestTypeIdentifier.BYPASS_HEADER_VALUE) ]
        List<Thread> threads = []
        threads = (1..30).collect {
            new TestMultiAccess("user", headers)
        }
        MultiThreadedTest.runTestThreads(threads)

        then: "all are successful"
        threads.size() == 30
        TestRateLimitFilter.instance.rateLimiter.globalCount.get() == 0
        TestMultiAccess.ok.get() == 30
        TestMultiAccess.fail.get() == 0
    }

    @Retry(count = 3, delay = 10)
    def "UI header triggers UI user limit"() {
        when: "UI user opens 20 simultaneous requests"
        Map headers = [
            referer: "http://localhost:${jtb.getHarness().getPort()}/",
            (DataApiRequestTypeIdentifier.CLIENT_HEADER_NAME) : (DataApiRequestTypeIdentifier.CLIENT_HEADER_VALUE)
        ]
        List<Thread> threads = []
        threads = (1..20).collect {
            new TestMultiAccess("user", headers)
        }
        MultiThreadedTest.runTestThreads(threads)

        then: "5 requests return failure 429"
        TestRateLimitFilter.instance.rateLimiter.globalCount.get() == 0
        TestRateLimitFilter.instance.rateLimiter.requestGlobalCounter.count == LIMIT_UI
        TestRateLimitFilter.instance.rateLimiter.requestBypassMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.requestUiMeter.count == LIMIT_UI
        TestRateLimitFilter.instance.rateLimiter.requestUserMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.rejectUiMeter.count == 20 - LIMIT_UI
        TestRateLimitFilter.instance.rateLimiter.rejectUserMeter.count == 0

        TestMultiAccess.ok.get() + TestMultiAccess.fail.get() == 20
        TestMultiAccess.ok.get() == LIMIT_UI
        TestMultiAccess.fail.get() == 20 - LIMIT_UI
    }

    def "UI OPTIONS unlimited"() {
        when: "For 30 OPTIONS requests"
        MultiThreadedTest.runTestThreadsOnce(TestOptions.class, 30)

        then: "0 requests return failure 429"
        TestRateLimitFilter.instance.rateLimiter.globalCount.get() == 0
        TestRateLimitFilter.instance.rateLimiter.requestGlobalCounter.count == 0
        TestRateLimitFilter.instance.rateLimiter.requestBypassMeter.count == 30
        TestRateLimitFilter.instance.rateLimiter.requestUiMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.requestUserMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.rejectUiMeter.count == 0
        TestRateLimitFilter.instance.rateLimiter.rejectUserMeter.count == 0

        TestOptions.ok.get() + TestOptions.fail.get() == 30
        TestOptions.ok.get() == 30
        TestOptions.fail.get() == 0
    }

    final static class TestOptions extends MultiThreadedTest.Once {

        static AtomicInteger ok = new AtomicInteger()
        static AtomicInteger fail = new AtomicInteger()

        void runTest() {

            int status = jtb.getHarness().target("test/data") .request().options().getStatus()

            switch (status) {
                case 200:
                    ok.incrementAndGet()
                    break
                case 429:
                    fail.incrementAndGet()
                    break
                default:
                    fail
            }
        }
    }
}
