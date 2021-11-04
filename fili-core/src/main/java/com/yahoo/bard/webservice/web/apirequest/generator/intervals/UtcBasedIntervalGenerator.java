// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.intervals;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_MISSING;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_ZERO_LENGTH;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_INTERVAL_GRANULARITY;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TIME_ALIGNMENT;
import static com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder.RequestResource.GRANULARITY;
import static com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder.RequestResource.INTERVALS;
import static com.yahoo.bard.webservice.web.apirequest.generator.intervals.IntervalElementType.DATE_TIME;
import static com.yahoo.bard.webservice.web.apirequest.generator.intervals.IntervalElementType.INVALID;
import static com.yahoo.bard.webservice.web.apirequest.generator.intervals.IntervalElementType.RRULE;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.generator.Generator;
import com.yahoo.bard.webservice.web.apirequest.generator.UnsatisfiedApiRequestConstraintsException;
import com.yahoo.bard.webservice.web.time.TimeMacro;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.dmfs.rfc5545.recur.RecurrenceRule;
import org.dmfs.rfc5545.recur.RecurrenceRuleIterator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Default generator implementation for {@link Interval}s. Intervals are always generated using the UTC timezone.
 * Intervals are dependent on {@link Granularity}, so ensure the granularity generator has bound the query granularity
 * before using this generator to bind the Intervals.
 *
 * Throws {@link UnsatisfiedApiRequestConstraintsException} if the granularity has not yet been bound.
 * Throws {@link BadApiRequestException} if no granularity was specified in the query.
 */
public class UtcBasedIntervalGenerator implements Generator<List<Interval>> {

    private static final Logger LOG = LoggerFactory.getLogger(UtcBasedIntervalGenerator.class);

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String RRULE_MAX_KEY = SYSTEM_CONFIG.getPackageVariableName("rrule_max_occurences");

    public static final String RRULE_INVALID_REASON =
            "Recurrence rule intervals must be used in the form of DATEEXPRESSION/RRULE=KEY=VALUE;KEY=VALUE...";

    public static final String AT_LEAST_ONE_DATE =
            "Interval expressions must contain at least one date as the first or second argument";

    public static final String INVALID_ELEMENT =
            "The intervals specified cannot be parsed.";

    public static final String RRULE_NEEDS_PERIOD =
            "RRule is only legal with a concrete time grain.";

    public static final String GRANULARITY_REQUIRED = "Granularity is required for all data queries, " +
            "but was not present in the request. Please add granularity to your query and try again.";


    @Override
    public List<Interval> bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        Granularity granularity = validateGranularityPresent(builder);

        return generateIntervals(
                new DateTime(),
                params.getIntervals().orElse(""),
                granularity,
                resources.getDateTimeFormatter()
        );
    }

    @Override
    public void validate(
            List<Interval> entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        Granularity granularity = validateGranularityPresent(builder);
        validateTimeAlignment(granularity, entity);
    }

    /**
     * Validates that the granularity has been bound and is present.
     *
     * @param builder The builder which contains all of the
     *
     * @return The granularity resolved.
     */
    protected Granularity validateGranularityPresent(DataApiRequestBuilder builder) {
        if (!builder.isGranularityInitialized()) {
            throw new UnsatisfiedApiRequestConstraintsException(
                    INTERVALS.getResourceName(),
                    Collections.singleton(GRANULARITY.getResourceName())
            );
        }
        if (!builder.getGranularityIfInitialized().isPresent()) {
            throw new BadApiRequestException(GRANULARITY_REQUIRED);
        }
        return builder.getGranularityIfInitialized().get();
    }

    /**
     * Extracts the set of intervals from the api request.
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
     *
     * @param now The 'now' for which time macros will be relatively calculated
     * @param apiIntervalQuery  API string containing the intervals in ISO 8601 format, values separated by ','.
     * @param granularity  The granularity to generate the date based on period or macros.
     * @param dateTimeFormatter  The formatter to parse date time interval segments
     *
     * @return Set of jodatime interval objects.
     * @throws BadApiRequestException if the requested interval is not found.
     */
    public static List<Interval> generateIntervals(
            DateTime now,
            String apiIntervalQuery,
            Granularity granularity,
            DateTimeFormatter dateTimeFormatter
    ) throws BadApiRequestException {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingIntervals")) {
            List<Interval> generated = new ArrayList<>();
            if (apiIntervalQuery == null || apiIntervalQuery.equals("")) {
                LOG.debug(INTERVAL_MISSING.logFormat());
                throw new BadApiRequestException(INTERVAL_MISSING.format());
            }
            String[] apiIntervals = apiIntervalQuery.split(",");
            // Split each interval string into the start and stop instances, parse them, and add the interval to the
            // list

            IntervalElement start;
            IntervalElement end;

            for (String apiInterval : apiIntervals) {
                String[] split = apiInterval.split("/");

                // Check for both a start and a stop
                if (split.length != 2) {
                    LOG.debug(INTERVAL_INVALID.logFormat(apiIntervalQuery, AT_LEAST_ONE_DATE));
                    throw new BadApiRequestException(INTERVAL_INVALID.format(apiIntervalQuery, AT_LEAST_ONE_DATE));
                }

                try {
                    start = new IntervalElement(now, split[0], granularity, dateTimeFormatter);
                    end = new IntervalElement(now, split[1], granularity, dateTimeFormatter);

                    if (start.type == INVALID || end.type == INVALID) {
                        LOG.debug(INTERVAL_INVALID.logFormat(start, INVALID_ELEMENT));
                        throw new BadApiRequestException(INTERVAL_INVALID.format(apiInterval, INVALID_ELEMENT));
                    }

                    //If start & end intervals are period then marking as invalid interval.
                    //Becacuse either one should be macro or actual date to generate an interval
                    if (start.type != DATE_TIME && end.type != DATE_TIME) {
                        LOG.debug(INTERVAL_INVALID.logFormat(start, AT_LEAST_ONE_DATE));
                        throw new BadApiRequestException(INTERVAL_INVALID.format(apiInterval, AT_LEAST_ONE_DATE));
                    }

                } catch (IllegalArgumentException iae) {
                    LOG.debug(INTERVAL_INVALID.logFormat(apiIntervalQuery, iae.getMessage()), iae);
                    throw new BadApiRequestException(INTERVAL_INVALID.format(apiIntervalQuery, iae.getMessage()), iae);
                }

                // RRULE legal use case is start with date time, end with rrule and granularity not all
                if (start.type == DATE_TIME && end.type == RRULE) {
                    if (granularity instanceof TimeGrain) {
                        // RRULE can't be zero length
                        List<Interval> recurrenceGenerated = buildIntervalFromRRule(
                                (TimeGrain) granularity,
                                start.dateTime,
                                end.rrule
                        );
                        generated.addAll(recurrenceGenerated);
                    } else {
                        LOG.debug(INTERVAL_INVALID.logFormat(apiIntervalQuery, RRULE_NEEDS_PERIOD));
                        throw new BadApiRequestException(INTERVAL_INVALID.format(apiIntervalQuery, RRULE_NEEDS_PERIOD));
                    }
                    continue;
                }

                Interval interval;

                try {
                    switch (start.type) {
                        case DATE_TIME:
                            switch (end.type) {
                                case DATE_TIME:
                                    interval = new Interval(start.dateTime, end.dateTime);
                                    break;
                                case PERIOD:
                                    interval = new Interval(start.dateTime, end.period);
                                    break;
                                default:
                                    LOG.debug(INTERVAL_INVALID.logFormat(start), INVALID_ELEMENT);
                                    String message = INTERVAL_INVALID.format(apiInterval, INVALID_ELEMENT);
                                    throw new BadApiRequestException(message);
                            }
                            break;
                        case PERIOD:
                            switch (end.type) {
                                case DATE_TIME:
                                    interval = new Interval(start.period, end.dateTime);
                                    break;
                                case PERIOD:
                                case RRULE:
                                    // Unreachable by at-least-one testing
                                default:
                                    // Hopefully Unreachable
                                    LOG.debug(INTERVAL_INVALID.logFormat(apiIntervalQuery, AT_LEAST_ONE_DATE));
                                    throw new BadApiRequestException(INTERVAL_INVALID.format(
                                            apiIntervalQuery,
                                            AT_LEAST_ONE_DATE
                                    ));
                            }
                            break;
                        case RRULE:
                            LOG.debug(INTERVAL_INVALID.logFormat(start), RRULE_INVALID_REASON);
                            String errorMessage = INTERVAL_INVALID.format(apiInterval, RRULE_INVALID_REASON);
                            throw new BadApiRequestException(errorMessage);
                        default:
                            // Unreachable by invalid processing
                            LOG.debug(INTERVAL_INVALID.logFormat(start));
                            throw new BadApiRequestException(INTERVAL_INVALID.format(apiInterval, INVALID_ELEMENT));
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
     * Build a set of intervals by applying an rrule to a start date given a granularity.
     *
     * @param tg a time grain with an associated period
     * @param start the start of the recurrance set
     * @param rule the generator rule for the recurrance set
     *
     * @return the intervals generated by this rule and this grain.
     */
    private static List<Interval> buildIntervalFromRRule(
            TimeGrain tg,
            DateTime start,
            RecurrenceRule rule
    ) {
        int maxInstances = SYSTEM_CONFIG.getIntProperty(RRULE_MAX_KEY, 100);
        org.dmfs.rfc5545.DateTime startTime = new org.dmfs.rfc5545.DateTime(start.getMillis());
        RecurrenceRuleIterator it = rule.iterator(startTime);

        List<Interval> results = new ArrayList<>();
        while (it.hasNext() && (maxInstances-- > 0)) {
            org.joda.time.DateTime dateTime = new org.joda.time.DateTime(
                    it.nextDateTime().getTimestamp(),
                    DateTimeZone.UTC
            );
            Interval i = new Interval(dateTime, tg.getPeriod());
            results.add(i);
        }
        return results;
    }

    /**
     * Get datetime from the given input text based on granularity.
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
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
                LOG.debug(INVALID_INTERVAL_GRANULARITY.logFormat(macro, dateText));
                throw new BadApiRequestException(INVALID_INTERVAL_GRANULARITY.format(macro, dateText));
            }
            return macro.getDateTime(now, (TimeGrain) granularity);
        }
        return DateTime.parse(dateText, timeFormatter);
    }


    /**
     * Throw an exception if any of the intervals are not accepted by this granularity.
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
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
            LOG.debug(TIME_ALIGNMENT.logFormat(intervals, granularity, alignmentDescription));
            throw new BadApiRequestException(TIME_ALIGNMENT.format(intervals, granularity, alignmentDescription));
        }
    }
}
