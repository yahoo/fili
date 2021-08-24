// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.intervals;

import com.yahoo.bard.webservice.data.time.Granularity;

import org.dmfs.rfc5545.recur.InvalidRecurrenceRuleException;
import org.dmfs.rfc5545.recur.RecurrenceRule;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;

public class IntervalElement {
    String rawText;
    DateTime dateTime;
    Period period;
    RecurrenceRule rrule;
    IntervalElementType type;

    public IntervalElement(
            DateTime now,
            String text,
            Granularity granularity,
            DateTimeFormatter dateTimeFormatter
    ) {
        rawText = text.toUpperCase(Locale.ENGLISH);
        type = IntervalElementType.parse(rawText);
        switch (type) {
            case DATE_TIME:
                dateTime = UtcBasedIntervalGenerator.getAsDateTime(now, granularity, rawText, dateTimeFormatter);
                period = null;
                rrule = null;
                break;
            case PERIOD:
                period = Period.parse(rawText);
                dateTime = null;
                rrule = null;
                break;
            case RRULE:
                rrule = parseRecurrenceRule(text);
                dateTime = null;
                period = null;
                break;
            case INVALID:
                dateTime = null;
                period = null;
                rrule = null;
        }
    }

    static RecurrenceRule parseRecurrenceRule(String text) {
        try {
            String rruleText = text.substring(IntervalElementType.RRULE_PREFIX.length());
            return new RecurrenceRule(rruleText);
        } catch (InvalidRecurrenceRuleException e) {
            throw new IllegalArgumentException(String.format("Illegal interval argument: %s", text), e);
        }
    }
}
