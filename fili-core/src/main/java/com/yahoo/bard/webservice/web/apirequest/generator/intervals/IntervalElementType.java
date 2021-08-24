// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.intervals;

import com.yahoo.bard.webservice.web.TimeMacro;

enum IntervalElementType {
    DATE_TIME,
    PERIOD,
    RRULE,
    INVALID;

    public static final String RRULE_PREFIX = "RRULE=";

    static IntervalElementType parse(String text) {
        char c = text.charAt(0);
        if (c == 'P') {
            return PERIOD;
        }
        if (Character.isDigit(c) || TimeMacro.forName(text) != null) {
            return DATE_TIME;
        }
        if (text.startsWith(RRULE_PREFIX)) {
            return RRULE;
        }
        return INVALID;
    }
}
