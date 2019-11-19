// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider

import spock.lang.Shared
import spock.lang.Specification

class DruidClientConfigHelperSpec extends Specification {

    private static final SystemConfig systemConfig = SystemConfigProvider.getInstance()

    private static final String DRUID_REQUEST_TIMEOUT_KEY = systemConfig.getPackageVariableName(
            "druid_request_timeout"
    )

    private static final String expectedUrl = "http://broker"

    private static final String expectedRequestTimeout ="600000"

    @Shared def url
    @Shared def requestTimeout

    def setupSpec() {
        url = systemConfig.getStringProperty(DruidClientConfigHelper.DRUID_BROKER_URL_KEY)

        requestTimeout = systemConfig.getStringProperty(DRUID_REQUEST_TIMEOUT_KEY, null)
        if (requestTimeout == null) {
            systemConfig.setProperty(DRUID_REQUEST_TIMEOUT_KEY, expectedRequestTimeout)
        }
    }

    def cleanupSpec() {
        if (url == null) {
            systemConfig.clearProperty(DruidClientConfigHelper.DRUID_BROKER_URL_KEY)
        } else {
            systemConfig.setProperty(DruidClientConfigHelper.DRUID_BROKER_URL_KEY , url)
        }

        if (requestTimeout == null) {
            systemConfig.clearProperty(DRUID_REQUEST_TIMEOUT_KEY)
        } else {
            systemConfig.setProperty(DRUID_REQUEST_TIMEOUT_KEY , requestTimeout)
        }
    }

    def "check if appropriate druid broker url is fetched"() {
        expect:
        DruidClientConfigHelper.getDruidUrl() == expectedUrl
    }


    def "check if appropriate druid request timeout is fetched"() {
        expect:
        DruidClientConfigHelper.getDruidTimeout() == Integer.parseInt(expectedRequestTimeout)
    }


    def "invalid url will throw illegal exception"() {
        when:
        DruidClientConfigHelper.validateUrl("[BAD URL]")

        then:
        IllegalArgumentException e = thrown()
        e.message == "Invalid druid host url provided: [BAD URL]"
    }
}
