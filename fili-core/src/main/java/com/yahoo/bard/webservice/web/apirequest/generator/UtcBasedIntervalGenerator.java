package com.yahoo.bard.webservice.web.apirequest.generator;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_MISSING;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_ZERO_LENGTH;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_INTERVAL_GRANULARITY;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TIME_ALIGNMENT;

import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.TimeMacro;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

//TODO depends on granularity. Must document this
public class UtcBasedIntervalGenerator implements Generator<List<Interval>>{

    private static final Logger LOG = LoggerFactory.getLogger(UtcBasedIntervalGenerator.class);

    @Override
    public List<Interval> bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        validateGranularityPresent(builder);

        return generateIntervals(
                new DateTime(),
                params.getIntervals().orElse(""),
                builder.getGranularity().get(),
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
        validateGranularityPresent(builder);
        validateTimeAlignment(builder.getGranularity().get(), entity);
    }

    private void validateGranularityPresent(DataApiRequestBuilder builder) {
        if (builder.getGranularity() == null) {
            // TODO standardize this error message inside the exception and have exception only consume the dependency names
            throw new UnsatisfiedApiRequestConstraintsException("Intervals depends on Granularity, but granularity " +
                    "has not been generated yet. Ensure the granularity generation stage always runs before the " +
                    "interval generation stage");
        }
        if (!builder.getGranularity().isPresent()) {
            throw new BadApiRequestException("Granularity is required for all data queries, but was not present in" +
                    "the request. Please add granularity to your query and try again.");
        }
    }

    /**
     * Extracts the set of intervals from the api request.
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
