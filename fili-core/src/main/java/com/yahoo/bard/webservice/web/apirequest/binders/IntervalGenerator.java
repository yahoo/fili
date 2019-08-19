// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.util.TableUtils;
import com.yahoo.bard.webservice.web.BadApiRequestException;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

/**
 * Generates intervals used by an ApiRequest implementation.
 */
public interface IntervalGenerator {

    /**
     * Bind the query interval string to a list of intervals.
     *
     * @param intervalsName  The query string describing the intervals
     * @param granularity  The granularity for this request
     * @param timeZone  The time zone to evaluate interval timestamps in
     * @param logicalTable  The logical table to source availability from.
     *
     * @return  A bound list of intervals for the query
     */
    List<Interval> generateIntervals(
            String intervalsName,
            Granularity granularity,
            DateTimeZone timeZone,
            LogicalTable logicalTable
    );

    /**
     * Bind the query interval string to a list of intervals.
     *
     * @param intervalsName  The query string describing the intervals
     * @param intervals  The bound intervals
     * @param granularity The request granularity
     * @param timeZone  The time zone to evaluate interval timestamps in
     *
     * @throws BadApiRequestException if invalid
     */
    void validateIntervals(
            String intervalsName,
            List<Interval> intervals,
            Granularity granularity,
            DateTimeZone timeZone
    );

    IntervalGenerator DEFAULT_INTERVAL_GENERATOR = new IntervalGenerator() {
        @Override
        public List<Interval> generateIntervals(
                String intervalsName,
                Granularity granularity,
                DateTimeZone timeZone,
                LogicalTable logicalTable
        ) {
            DateTimeFormatter dateTimeFormatter = IntervalGenerationUtils.generateDateTimeFormatter(timeZone);
            List<Interval> result;

            SimplifiedIntervalList availability = TableUtils.logicalTableAvailability(logicalTable);
            DateTime adjustedNow = new DateTime();

            if (BardFeatureFlag.CURRENT_TIME_ZONE_ADJUSTMENT.isOn()) {
                adjustedNow = IntervalGenerationUtils.getAdjustedTime(adjustedNow);
                result = IntervalGenerationUtils
                        .generateIntervals(adjustedNow, intervalsName, granularity, dateTimeFormatter
                );
            } else if (BardFeatureFlag.CURRENT_MACRO_USES_LATEST.isOn()) {
                if (! availability.isEmpty()) {
                    DateTime firstUnavailable =  availability.getLast().getEnd();
                    if (firstUnavailable.isBeforeNow()) {
                        adjustedNow = firstUnavailable;
                    }
                }
                result = IntervalGenerationUtils
                        .generateIntervals(adjustedNow, intervalsName, granularity, dateTimeFormatter);
            } else {
                result = IntervalGenerationUtils.generateIntervals(intervalsName, granularity, dateTimeFormatter);
            }
            return result;
        }

        @Override
        public void validateIntervals(
                String intervalsName,
                List<Interval> intervals,
                Granularity granularity,
                DateTimeZone timeZone
        ) {
            IntervalGenerationUtils.validateTimeAlignment(granularity, intervals);
        }
    };
}
