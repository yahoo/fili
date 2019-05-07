// Copyright 2019 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.web.apirequest.binders.IntervalBinders

import org.joda.time.DateTime

import spock.lang.Specification
import spock.lang.Unroll

class IntervalBindersSpec extends Specification {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance()

    @Unroll
    def "Test adjustment of server time when #description"() {

        when:

        SYSTEM_CONFIG.setProperty("bard__adjusted_time_zone",zone)
        DateTime adjustedNow = IntervalBinders.getAdjustedTime(new DateTime(serverTime))

        then:
        adjustedNow.toString() == expectedDateTime

        where:
        serverTime                          |   expectedDateTime                    | zone                  | description
        "2019-04-24T00:13:00.564Z"          |   "2019-04-23T17:13:00.564Z"          | "America/Los_Angeles" | "server time is past midnight UTC and timezone to adjust is PST"
        "2019-04-23T19:13:00.564Z"          |   "2019-04-23T19:13:00.564Z"          | "UTC"                 | "server time is in UTC and timezone to adjust to is also UTC"
        "2019-04-23T10:13:00.564Z"          |   "2019-04-23T03:13:00.564Z"          | "America/Los_Angeles" | "server time is before midnight UTC and timezone to adjust is PST"
    }

}
