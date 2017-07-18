// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.data.volatility

import com.yahoo.fili.webservice.util.SimplifiedIntervalList

import spock.lang.Specification

/**
 * Test that the default instance for returns no intervals
 */
class NoVolatileIntervalsFunctionSpec extends Specification {

    def "getVolatileIntervals returns empty intervals"() {
        expect:
        new SimplifiedIntervalList() == NoVolatileIntervalsFunction.INSTANCE.getVolatileIntervals()
    }
}
