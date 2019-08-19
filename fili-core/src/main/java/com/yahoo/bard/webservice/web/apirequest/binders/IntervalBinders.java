// Copyright 2019 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_MISSING;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_ZERO_LENGTH;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_INTERVAL_GRANULARITY;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TIME_ALIGNMENT;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.TimeMacro;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * This utility class adjusts the DateTime for bindIntervals method of DataApiRequestImpl.
 */
public final class IntervalBinders {

    protected static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String ADJUSTED_TIME_ZONE_KEY = "adjusted_time_zone";

    /**
     * Private constructor to prevent instantiation.
     */
    private IntervalBinders() {
        throw new AssertionError("IntervalBinders is a nonstantiable util class");
    }

    /**
     * Adjusts the DateTime for the query interval.
     *
     * @param dateTime  current dateTime
     *
     * @return either adjusted dateTime or the current dateTime as is.
     */
    public static DateTime getAdjustedTime (DateTime dateTime) {
        String adjustedTimeZone = SYSTEM_CONFIG.getStringProperty(SYSTEM_CONFIG.getPackageVariableName(
                ADJUSTED_TIME_ZONE_KEY), "UTC"
        );
        return dateTime.withZone(DateTimeZone.forID(adjustedTimeZone))
                       .withZoneRetainFields(DateTimeZone.UTC);
    }

    /**
     * Extracts the set of intervals from the query parameters. Uses system datetime as the default 'now' for
     * calculating time macros.
     *
     * @param intervalQuery  API string containing the intervals in ISO 8601 format, values separated by ','.
     * @param granularity  The granularity to generate the date based on period or macros.
     * @param dateTimeFormatter  The formatter to parse date time interval segments
     *
     * @return Set of jodatime interval objects.
     * @throws BadApiRequestException if the requested interval is not found.
     */
    public static List<Interval> generateIntervals(
            String intervalQuery,
            Granularity granularity,
            DateTimeFormatter dateTimeFormatter
    ) throws BadApiRequestException {
        return generateIntervals(new DateTime(), intervalQuery, granularity, dateTimeFormatter);
    }


    /**
     * Extracts the set of intervals from the query parameters.
     *
     * @param now The 'now' for which time macros will be relatively calculated
     * @param intervalQuery  String containing the requested intervals in ISO 8601 format, values separated by ','.
     * @param granularity  The granularity to generate the date based on period or macros.
     * @param dateTimeFormatter  The formatter to parse date time interval segments
     *
     * @return Set of jodatime interval objects.
     * @throws BadApiRequestException if the requested interval is not found.
     */
    public static List<Interval> generateIntervals(
            DateTime now,
            String intervalQuery,
            Granularity granularity,
            DateTimeFormatter dateTimeFormatter
    ) throws BadApiRequestException {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingIntervals")) {
            List<Interval> generated = new ArrayList<>();
            if (intervalQuery == null || intervalQuery.equals("")) {
                throw new BadApiRequestException(INTERVAL_MISSING.format());
            }
            List<String> apiIntervals = Arrays.asList(intervalQuery.split(","));
            // Split each interval string into the start and stop instances, parse them, and add the interval to the
            // list

            for (String apiInterval : apiIntervals) {
                String[] split = apiInterval.split("/");

                // Check for both a start and a stop
                if (split.length != 2) {
                    String message = "Start and End dates are required.";
                    throw new BadApiRequestException(INTERVAL_INVALID.format(intervalQuery, message));
                }

                try {
                    String start = split[0].toUpperCase(Locale.ENGLISH);
                    String end = split[1].toUpperCase(Locale.ENGLISH);
                    //If start & end intervals are period then marking as invalid interval.
                    //Becacuse either one should be macro or actual date to generate an interval
                    if (start.startsWith("P") && end.startsWith("P")) {
                        throw new BadApiRequestException(INTERVAL_INVALID.format(apiInterval));
                    }

                    Interval interval;
                    //If start interval is period, then create new interval with computed end date
                    //possible end interval could be next,current, date
                    if (start.startsWith("P")) {
                        interval = new Interval(
                                Period.parse(start),
                                getAsDateTime(now, granularity, split[1], dateTimeFormatter)
                        );
                        //If end string is period, then create an interval with the computed start date
                        //Possible start & end string could be a macro or an ISO 8601 DateTime
                    } else if (end.startsWith("P")) {
                        interval = new Interval(
                                getAsDateTime(now, granularity, split[0], dateTimeFormatter),
                                Period.parse(end)
                        );
                    } else {
                        //start and end interval could be either macros or actual datetime
                        interval = new Interval(
                                getAsDateTime(now, granularity, split[0], dateTimeFormatter),
                                getAsDateTime(now, granularity, split[1], dateTimeFormatter)
                        );
                    }

                    // Zero length intervals are invalid
                    if (interval.toDuration().equals(Duration.ZERO)) {
                        throw new BadApiRequestException(INTERVAL_ZERO_LENGTH.format(apiInterval));
                    }
                    generated.add(interval);
                } catch (IllegalArgumentException iae) {
                    // Handle poor JodaTime message (special case)
                    String internalMessage = iae.getMessage().equals("The end instant must be greater the start") ?
                            "The end instant must be greater than the start instant" :
                            iae.getMessage();
                    throw new BadApiRequestException(INTERVAL_INVALID.format(intervalQuery, internalMessage), iae);
                }
            }
            return generated;
        }
    }

    /**
     * Get datetime from the given input text based on granularity.
     *
     * @param now  current datetime to compute the floored date based on granularity
     * @param granularity  granularity to truncate the given date to.
     * @param dateText  start/end date text which could be actual date or macros
     * @param timeFormatter  a time zone adjusted date time formatter
     *
     * @return joda datetime of the given start/end date text or macros
     *
     * @throws BadApiRequestException if the granularity is "all" and a macro is used
     */
    public static DateTime getAsDateTime(
            DateTime now,
            Granularity granularity,
            String dateText,
            DateTimeFormatter timeFormatter
    ) throws BadApiRequestException {
        //If granularity is all and dateText is macro, then throw an exception
        TimeMacro macro = TimeMacro.forName(dateText);
        if (macro != null) {
            if (granularity instanceof AllGranularity) {
                throw new BadApiRequestException(INVALID_INTERVAL_GRANULARITY.format(macro, dateText));
            }
            return macro.getDateTime(now, (TimeGrain) granularity);
        }
        return DateTime.parse(dateText, timeFormatter);
    }

    /**
     * Throw an exception if any of the intervals are not accepted by this granularity.
     *
     * @param granularity  The granularity whose alignment is being tested.
     * @param intervals  The intervals being tested.
     *
     * @throws BadApiRequestException if the granularity does not align to the intervals
     */
    public static void validateTimeAlignment(
            Granularity granularity,
            List<Interval> intervals
    ) throws BadApiRequestException {
        if (!granularity.accepts(intervals)) {
            String alignmentDescription = granularity.getAlignmentDescription();
            throw new BadApiRequestException(TIME_ALIGNMENT.format(intervals, granularity, alignmentDescription));
        }
    }
}
