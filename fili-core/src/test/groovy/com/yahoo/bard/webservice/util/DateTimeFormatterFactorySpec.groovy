// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormatter

import spock.lang.Specification
import spock.lang.Unroll

class DateTimeFormatterFactorySpec extends Specification {

    SystemConfig systemConfig = SystemConfigProvider.getInstance()

    @Unroll
    def "check parsing string #string parses to #expected"() {

        expect: "It parses to the interval we expect"
        DateTime.parse(string, DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER) == expected

        where:
        string                    | expected
        "2005"                    | new DateTime(2005, 01, 01, 00, 00, 00, 000)
        "2005-03"                 | new DateTime(2005, 03, 01, 00, 00, 00, 000)
        "2005-03-25"              | new DateTime(2005, 03, 25, 00, 00, 00, 000)

        "2005-03-25T10"           | new DateTime(2005, 03, 25, 10, 00, 00, 000)
        "2005-03-25T10:20"        | new DateTime(2005, 03, 25, 10, 20, 00, 000)
        "2005-03-25T10:20:30"     | new DateTime(2005, 03, 25, 10, 20, 30, 000)
        "2005-03-25T10:20:30.555" | new DateTime(2005, 03, 25, 10, 20, 30, 555)

        "2005-03-25 10"           | new DateTime(2005, 03, 25, 10, 00, 00, 000)
        "2005-03-25 10:20"        | new DateTime(2005, 03, 25, 10, 20, 00, 000)
        "2005-03-25 10:20:30"     | new DateTime(2005, 03, 25, 10, 20, 30, 000)
        "2005-03-25 10:20:30.555" | new DateTime(2005, 03, 25, 10, 20, 30, 555)
    }


    @Unroll
    def "check parsing date parses to #expected with #format in #timeZone"() {
        setup:
        DateTimeZone originalDateTime = DateTimeZone.getDefault()
        DateTimeZone.setDefault(timeZone)

        DateTime date = new DateTime(2005, 03, 25, 10, 20, 30, 555)
        DateTimeFormatter originalOutputFormatter = DateTimeFormatterFactory.DATETIME_OUTPUT_FORMATTER
        DateTimeFormatterFactory.DATETIME_OUTPUT_FORMATTER = null
        systemConfig.setProperty(DateTimeFormatterFactory.OUTPUT_DATETIME_FORMAT, format)

        expect: "It parses to the interval we expect"
        date.toString(DateTimeFormatterFactory.getOutputFormatter()) == expected

        cleanup:
        systemConfig.clearProperty(DateTimeFormatterFactory.OUTPUT_DATETIME_FORMAT)
        DateTimeFormatterFactory.DATETIME_OUTPUT_FORMATTER = originalOutputFormatter
        DateTimeZone.setDefault(originalDateTime)

        where:
        expected                               | format                        |  timeZone
        "2005"                                 | "YYYY"                        |  DateTimeZone.UTC
        "2005-03"                              | "YYYY-MM"                     |  DateTimeZone.UTC
        "2005-03-25"                           | "YYYY-MM-dd"                  |  DateTimeZone.UTC

        "2005-03-25 10:20:30.555"              | "YYYY-MM-dd HH:mm:ss.SSS"     |  DateTimeZone.UTC
        "2005-03-25 10:20:30.555+0000"         | "YYYY-MM-dd HH:mm:ss.SSSZ"    |  DateTimeZone.UTC
        "2005-03-25 10:20:30.555+00:00"        | "YYYY-MM-dd HH:mm:ss.SSSZZ"   |  DateTimeZone.UTC

        "2005-03-25 10:20:30.555-0600"         | "YYYY-MM-dd HH:mm:ss.SSSZ"    |  DateTimeZone.forID("-06:00")
        "2005-03-25 10:20:30.555-06:00"        | "YYYY-MM-dd HH:mm:ss.SSSZZ"   |  DateTimeZone.forID("-06:00")
        "2005-03-25 10:20:30.555 CST"          | "YYYY-MM-dd HH:mm:ss.SSS z"   |  DateTimeZone.forID("America/Chicago")

        "2005-03-25 10:20:30.555 Europe/Rome"  | "YYYY-MM-dd HH:mm:ss.SSS ZZZ" |  DateTimeZone.forID("Europe/Rome")
    }
}
