// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.util.DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_MISSING;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_ZERO_LENGTH;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_INTERVAL_GRANULARITY;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_TIME_ZONE;

import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.TimeMacro;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A utility class for generators for time and intervals.
 */
public class DateAndTimeGenerators {
    private static final Logger LOG = LoggerFactory.getLogger(DateAndTimeGenerators.class);

    public static DateAndTimeGenerators INSTANCE = new DateAndTimeGenerators();

    /**
     * Extracts the set of intervals from the api request.
     *
     * @param apiIntervalQuery  API string containing the intervals in ISO 8601 format, values separated by ','.
     * @param granularity  The granularity to generate the date based on period or macros.
     * @param dateTimeFormatter  The formatter to parse date time interval segments
     *
     * @return Set of jodatime interval objects.
     * @throws BadApiRequestException if the requested interval is not found.
     */
    public List<Interval> generateIntervals(
            String apiIntervalQuery,
            Granularity granularity,
            DateTimeFormatter dateTimeFormatter
    ) throws BadApiRequestException {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingIntervals")) {
            Set<Interval> generated = new LinkedHashSet<>();
            if (apiIntervalQuery == null || apiIntervalQuery.equals("")) {
                LOG.debug(INTERVAL_MISSING.logFormat());
                throw new BadApiRequestException(INTERVAL_MISSING.format());
            }
            List<String> apiIntervals = Arrays.asList(apiIntervalQuery.split(","));
            // Split each interval string into the start and stop instances, parse them, and add the interval to the
            // list

            for (String apiInterval : apiIntervals) {
                String[] split = apiInterval.split("/");

                // Check for both a start and a stop
                if (split.length != 2) {
                    String message = "Start and End dates are required.";
                    LOG.debug(INTERVAL_INVALID.logFormat(apiIntervalQuery, message));
                    throw new BadApiRequestException(INTERVAL_INVALID.format(apiIntervalQuery, message));
                }

                try {
                    String start = split[0].toUpperCase(Locale.ENGLISH);
                    String end = split[1].toUpperCase(Locale.ENGLISH);
                    //If start & end intervals are period then marking as invalid interval.
                    //Becacuse either one should be macro or actual date to generate an interval
                    if (start.startsWith("P") && end.startsWith("P")) {
                        LOG.debug(INTERVAL_INVALID.logFormat(start));
                        throw new BadApiRequestException(INTERVAL_INVALID.format(apiInterval));
                    }

                    Interval interval;
                    DateTime now = new DateTime();
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
                        LOG.debug(INTERVAL_ZERO_LENGTH.logFormat(apiInterval));
                        throw new BadApiRequestException(INTERVAL_ZERO_LENGTH.format(apiInterval));
                    }
                    generated.add(interval);
                } catch (IllegalArgumentException iae) {
                    // Handle poor JodaTime message (special case)
                    String internalMessage = iae.getMessage().equals("The end instant must be greater the start") ?
                            "The end instant must be greater than the start instant" :
                            iae.getMessage();
                    LOG.debug(INTERVAL_INVALID.logFormat(apiIntervalQuery, internalMessage), iae);
                    throw new BadApiRequestException(INTERVAL_INVALID.format(apiIntervalQuery, internalMessage), iae);
                }
            }
            return new SimplifiedIntervalList(generated);
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
    public DateTime getAsDateTime(
            DateTime now,
            Granularity granularity,
            String dateText,
            DateTimeFormatter timeFormatter
    ) throws BadApiRequestException {
        //If granularity is all and dateText is macro, then throw an exception
        TimeMacro macro = TimeMacro.forName(dateText);
        if (macro != null) {
            if (granularity instanceof AllGranularity) {
                LOG.debug(INVALID_INTERVAL_GRANULARITY.logFormat(macro, dateText));
                throw new BadApiRequestException(INVALID_INTERVAL_GRANULARITY.format(macro, dateText));
            }
            return macro.getDateTime(now, (TimeGrain) granularity);
        }
        return DateTime.parse(dateText, timeFormatter);
    }

    /**
     * Get the DateTimeFormatter shifted to the given time zone.
     *
     * @param timeZone  TimeZone to shift the default formatter to
     *
     * @return the timezone-shifted formatter
     */
    public static DateTimeFormatter generateDateTimeFormatter(DateTimeZone timeZone) {
        return FULLY_OPTIONAL_DATETIME_FORMATTER.withZone(timeZone);
    }


    /**
     * Get the timezone for the request.
     *
     * @param timeZoneId  String of the TimeZone ID
     * @param systemTimeZone  TimeZone of the system to use if there is no timeZoneId
     *
     * @return the request's TimeZone
     */
    protected DateTimeZone generateTimeZone(String timeZoneId, DateTimeZone systemTimeZone) {
        try (TimedPhase timer = RequestLog.startTiming("generatingTimeZone")) {
            if (timeZoneId == null) {
                return systemTimeZone;
            }
            try {
                return DateTimeZone.forID(timeZoneId);
            } catch (IllegalArgumentException ignored) {
                LOG.debug(INVALID_TIME_ZONE.logFormat(timeZoneId));
                throw new BadApiRequestException(INVALID_TIME_ZONE.format(timeZoneId));
            }
        }
    }
}
