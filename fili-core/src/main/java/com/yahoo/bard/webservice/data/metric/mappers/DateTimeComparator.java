// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

import org.joda.time.DateTime;

import java.util.Comparator;

/**
 * Comparator to compare two dateTime.
 */
public class DateTimeComparator implements Comparator<DateTime> {

    @Override
    public int compare(DateTime date1, DateTime date2) {
        if (date1.toDateTime() == null || date2.toDateTime() == null) {
            return 0;
        }
        return date1.toDateTime().compareTo(date2.toDateTime());
    }
}
