// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.volatility

import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import spock.lang.Specification

/**
 * Test that the default instance for returns no intervals
 */
class NoVolatileIntervalsFunctionSpec extends Specification {

    def "getVolatileIntervals returns empty intervals"() {
        expect:
        SimplifiedIntervalList.NO_INTERVALS == NoVolatileIntervalsFunction.INSTANCE.getVolatileIntervals()
    }
}
