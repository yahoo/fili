// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.ACCEPT_FORMAT_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_NOT_IN_TABLE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_MISSING;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTERVAL_ZERO_LENGTH;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_ASYNC_AFTER;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_INTERVAL_GRANULARITY;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_TIME_ZONE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_NOT_IN_TABLE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_GRANULARITY_MISMATCH;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TIME_ALIGNMENT;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.UNKNOWN_GRANULARITY;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.util.AllPagesPagination;
import com.yahoo.bard.webservice.util.GranularityParseException;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.BadPaginationException;
import com.yahoo.bard.webservice.web.DefaultResponseFormatType;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.TimeMacro;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.PathSegment;

/**
 * API Request. Abstract class offering default implementations for the common components of API request objects.
 */
public abstract class ApiRequestImpl implements ApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(ApiRequestImpl.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    protected static final String COMMA_AFTER_BRACKET_PATTERN = "(?<=]),";

    private static final int DEFAULT_PER_PAGE = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("default_per_page")
    );
    private static final int DEFAULT_PAGE = 1;
    public static final PaginationParameters DEFAULT_PAGINATION = new PaginationParameters(
            DEFAULT_PER_PAGE,
            DEFAULT_PAGE
    );
    protected static final String SYNCHRONOUS_REQUEST_FLAG = "never";
    protected static final String ASYNCHRONOUS_REQUEST_FLAG = "always";

    protected final ResponseFormatType format;
    protected final Optional<PaginationParameters> paginationParameters;
    protected final long asyncAfter;
    protected final String downloadFilename;

    /**
     * Parses the API request URL and generates the API request object.
     *
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param asyncAfter  How long the user is willing to wait for a synchronous request in milliseconds, if null
     * defaults to the system config {@code default_asyncAfter}
     * @param perPage  number of rows to display per page of results. If present in the original request, must be a
     * positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive integer. If not
     * present, must be the empty string.
     *
     * @throws BadApiRequestException if pagination parameters in the API request are not positive integers.
     * @deprecated prefer constructor with downloadFilename
     */
    @Deprecated
    public ApiRequestImpl(
            String format,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page
    ) throws BadApiRequestException {
        this(format, null, asyncAfter, perPage, page);
    }

    /**
     * Parses the API request URL and generates the API request object.
     *
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param asyncAfter  How long the user is willing to wait for a synchronous request in milliseconds, if null
     * defaults to the system config {@code default_asyncAfter}
     * @param downloadFilename  The filename for the response to be downloaded as. If null or empty indicates response
     * should not be downloaded.
     * @param perPage  number of rows to display per page of results. If present in the original request, must be a
     * positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive integer. If not
     * present, must be the empty string.
     *
     * @throws BadApiRequestException if pagination parameters in the API request are not positive integers.
     */
    public ApiRequestImpl(
            String format,
            String downloadFilename,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page
    ) throws BadApiRequestException {
        this.format = generateAcceptFormat(format);
        this.paginationParameters = generatePaginationParameters(perPage, page);
        this.asyncAfter = generateAsyncAfter(
                asyncAfter == null ?
                        SYSTEM_CONFIG.getStringProperty(SYSTEM_CONFIG.getPackageVariableName("default_asyncAfter")) :
                        asyncAfter
        );
        this.downloadFilename = downloadFilename;
    }

    /**
     * Parses the API request URL and generates the API request object. Defaults asyncAfter to never.
     *
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param perPage  number of rows to display per page of results. If present in the original request, must be a
     * positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive integer. If not
     * present, must be the empty string.
     *
     * @throws BadApiRequestException if pagination parameters in the API request are not positive integers.
     */
    public ApiRequestImpl(
            String format,
            @NotNull String perPage,
            @NotNull String page
    ) throws BadApiRequestException {
        this(format, null, SYNCHRONOUS_REQUEST_FLAG, perPage, page);
    }

    /**
     * All argument constructor, meant to be used for rewriting apiRequest.
     *
     * @param format  The format of the response
     * @param asyncAfter  How long the user is willing to wait for a synchronous request, in milliseconds
     * @param paginationParameters  The parameters used to describe pagination
     * @deprecated prefer constructor with downloadFilename
     */
    @Deprecated
    protected ApiRequestImpl(
            ResponseFormatType format,
            long asyncAfter,
            Optional<PaginationParameters> paginationParameters
    ) {
        this.format = format;
        this.asyncAfter = asyncAfter;
        this.paginationParameters = paginationParameters;
        this.downloadFilename = null;
    }

    /**
     * All argument constructor, meant to be used for rewriting apiRequest.
     *
     * @param format  The format of the response
     * @param downloadFilename If not null and not empty, indicates the response should be downloaded by the client with
     * the provided filename. Otherwise indicates the response should be rendered in the browser.
     * @param asyncAfter  How long the user is willing to wait for a synchronous request, in milliseconds
     * @param paginationParameters  The parameters used to describe pagination
     */
    protected ApiRequestImpl(
            ResponseFormatType format,
            String downloadFilename,
            long asyncAfter,
            Optional<PaginationParameters> paginationParameters
    ) {
        this.format = format;
        this.downloadFilename = downloadFilename;
        this.asyncAfter = asyncAfter;
        this.paginationParameters = paginationParameters;
    }

    /**
     * Generate a Granularity instance based on a path element.
     *
     * @param granularity  A string representation of the granularity
     * @param dateTimeZone  The time zone to use for this granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance with time zone information
     * @throws BadApiRequestException if the string matches no meaningful granularity
     */
    protected Granularity generateGranularity(
            @NotNull String granularity,
            @NotNull DateTimeZone dateTimeZone,
            @NotNull GranularityParser granularityParser
    ) throws BadApiRequestException {
        try {
            return granularityParser.parseGranularity(granularity, dateTimeZone);
        } catch (GranularityParseException e) {
            LOG.error(UNKNOWN_GRANULARITY.logFormat(granularity), granularity);
            throw new BadApiRequestException(e.getMessage());
        }
    }

    /**
     * Generate a Granularity instance based on a path element.
     *
     * @param granularity  A string representation of the granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance without time zone information
     * @throws BadApiRequestException if the string matches no meaningful granularity
     */
    protected Granularity generateGranularity(String granularity, GranularityParser granularityParser)
            throws BadApiRequestException {
        try {
            return granularityParser.parseGranularity(granularity);
        } catch (GranularityParseException e) {
            LOG.error(UNKNOWN_GRANULARITY.logFormat(granularity), granularity);
            throw new BadApiRequestException(e.getMessage(), e);
        }
    }

    /**
     * Extracts the list of dimension names from the url dimension path segments and generates a set of dimension
     * objects based on it.
     *
     * @param apiDimensions  Dimension path segments from the URL.
     * @param dimensionDictionary  Dimension dictionary contains the map of valid dimension names and dimension objects.
     *
     * @return Set of dimension objects.
     * @throws BadApiRequestException if an invalid dimension is requested.
     */
    protected LinkedHashSet<Dimension> generateDimensions(
            List<PathSegment> apiDimensions,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingDimensions")) {
            // Dimensions are optional hence check if dimensions are requested.
            if (apiDimensions == null || apiDimensions.isEmpty()) {
                return new LinkedHashSet<>();
            }

            // set of dimension names (strings)
            List<String> dimApiNames = apiDimensions.stream()
                    .map(PathSegment::getPath)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());

            // set of dimension objects
            LinkedHashSet<Dimension> generated = new LinkedHashSet<>();
            List<String> invalidDimensions = new ArrayList<>();
            for (String dimApiName : dimApiNames) {
                Dimension dimension = dimensionDictionary.findByApiName(dimApiName);

                // If dimension dictionary returns a null, it means the requested dimension is not found.
                if (dimension == null) {
                    invalidDimensions.add(dimApiName);
                } else {
                    generated.add(dimension);
                }
            }

            if (!invalidDimensions.isEmpty()) {
                LOG.debug(DIMENSIONS_UNDEFINED.logFormat(invalidDimensions.toString()));
                throw new BadApiRequestException(DIMENSIONS_UNDEFINED.format(invalidDimensions.toString()));
            }

            LOG.trace("Generated set of dimension: {}", generated);
            return generated;
        }
    }

    /**
     * Ensure all request dimensions are part of the logical table.
     *
     * @param requestDimensions  The dimensions being requested
     * @param table  The logical table being checked
     *
     * @throws BadApiRequestException if any of the dimensions do not match the logical table
     */
    protected void validateRequestDimensions(Set<Dimension> requestDimensions, LogicalTable table)
            throws BadApiRequestException {
        // Requested dimensions must lie in the logical table
        requestDimensions = new HashSet<>(requestDimensions);
        requestDimensions.removeAll(table.getDimensions());

        if (!requestDimensions.isEmpty()) {
            List<String> dimensionNames = requestDimensions.stream()
                    .map(Dimension::getApiName)
                    .collect(Collectors.toList());
            LOG.debug(DIMENSIONS_NOT_IN_TABLE.logFormat(dimensionNames, table.getName()));
            throw new BadApiRequestException(DIMENSIONS_NOT_IN_TABLE.format(dimensionNames, table.getName()));
        }
    }

    /**
     * Given a single dimension filter string, generate a metric name extension.
     *
     * @param filterString  Single dimension filter string.
     *
     * @return Metric name extension created for the filter.
     */
    protected String generateMetricName(String filterString) {
        return filterString.replace("|", "_").replace("-", "_").replace(",", "_").replace("]", "").replace("[", "_");
    }

    /**
     * Extracts the list of metrics from the url metric query string and generates a set of LogicalMetrics.
     * <p>
     * If the query contains undefined metrics, {@link com.yahoo.bard.webservice.web.BadApiRequestException} will be
     * thrown.
     *
     * @param apiMetricQuery  URL query string containing the metrics separated by ','
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects
     *
     * @return set of metric objects
     */
    protected LinkedHashSet<LogicalMetric> generateLogicalMetrics(
            String apiMetricQuery,
            MetricDictionary metricDictionary
    ) {
        LinkedHashSet<LogicalMetric> metrics = new LinkedHashSet<>();
        List<String> invalidMetricNames = new ArrayList<>();

        String[] parsedMetrics = apiMetricQuery.split(",");
        if (parsedMetrics.length == 1 && parsedMetrics[0].isEmpty()) {
            parsedMetrics = new String[0];
        }

        // TODO extract into checkInvalidMetricNames method
        for (String metricName : parsedMetrics) {
            LogicalMetric logicalMetric = metricDictionary.get(metricName);
            if (logicalMetric == null) {
                invalidMetricNames.add(metricName);
            } else {
                metrics.add(logicalMetric);
            }
        }
        if (!invalidMetricNames.isEmpty()) {
            String message = ErrorMessageFormat.METRICS_UNDEFINED.logFormat(invalidMetricNames);
            LOG.error(message);
            throw new BadApiRequestException(message);
        }
        return metrics;
    }

    /**
     * Validate that all metrics are part of the logical table.
     *
     * @param logicalMetrics  The set of metrics being validated
     * @param table  The logical table for the request
     *
     * @throws BadApiRequestException if the requested metrics are not in the logical table
     */
    protected void validateMetrics(Set<LogicalMetric> logicalMetrics, LogicalTable table)
            throws BadApiRequestException {
        //get metric names from the logical table
        Set<String> validMetricNames = table.getLogicalMetrics().stream()
                .map(LogicalMetric::getName)
                .collect(Collectors.toSet());

        //get metric names from logicalMetrics and remove all the valid metrics
        Set<String> invalidMetricNames = logicalMetrics.stream()
                .map(LogicalMetric::getName)
                .filter(it -> !validMetricNames.contains(it))
                .collect(Collectors.toSet());

        //requested metrics names are not present in the logical table metric names set
        if (!invalidMetricNames.isEmpty()) {
            LOG.debug(METRICS_NOT_IN_TABLE.logFormat(invalidMetricNames, table.getName()));
            throw new BadApiRequestException(
                    METRICS_NOT_IN_TABLE.format(invalidMetricNames, table.getName())
            );
        }
    }

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
    protected static List<Interval> generateIntervals(
            String apiIntervalQuery,
            Granularity granularity,
            DateTimeFormatter dateTimeFormatter
    ) throws BadApiRequestException {
        return generateIntervals(new DateTime(), apiIntervalQuery, granularity, dateTimeFormatter);
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
    protected static List<Interval> generateIntervals(
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

    /**
     * Throw an exception if any of the intervals are not accepted by this granularity.
     *
     * @param granularity  The granularity whose alignment is being tested.
     * @param intervals  The intervals being tested.
     *
     * @throws BadApiRequestException if the granularity does not align to the intervals
     */
    protected static void validateTimeAlignment(
            Granularity granularity,
            List<Interval> intervals
    ) throws BadApiRequestException {
        if (!granularity.accepts(intervals)) {
            String alignmentDescription = granularity.getAlignmentDescription();
            LOG.debug(TIME_ALIGNMENT.logFormat(intervals, granularity, alignmentDescription));
            throw new BadApiRequestException(TIME_ALIGNMENT.format(intervals, granularity, alignmentDescription));
        }
    }

    /**
     * Generates the format in which the response data is expected.
     *
     * @param format  Expects a URL format query String.
     *
     * @return Response format type (CSV or JSON).
     * @throws BadApiRequestException if the requested format is not found.
     */
    protected ResponseFormatType generateAcceptFormat(String format) throws BadApiRequestException {
        try {
            return format == null ?
                    DefaultResponseFormatType.JSON :
                    DefaultResponseFormatType.valueOf(format.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            LOG.error(ACCEPT_FORMAT_INVALID.logFormat(format), e);
            throw new BadApiRequestException(ACCEPT_FORMAT_INVALID.format(format));
        }
    }

    /**
     * Builds the paginationParameters object, if the request provides both a perPage and page field.
     *
     * @param perPage  The number of rows per page.
     * @param page  The page to display.
     *
     * @return An Optional wrapping a PaginationParameters if both 'perPage' and 'page' exist.
     * @throws BadApiRequestException if 'perPage' or 'page' is not a positive integer, or if either one is empty
     * string but not both.
     */
    protected Optional<PaginationParameters> generatePaginationParameters(String perPage, String page)
            throws BadApiRequestException {
        try {
            return "".equals(perPage) && "".equals(page) ?
                    Optional.empty() :
                    Optional.of(new PaginationParameters(perPage, page));
        } catch (BadPaginationException invalidParameters) {
            throw new BadApiRequestException(invalidParameters.getMessage());
        }
    }

    /**
     * Extracts a specific logical table object given a valid table name and a valid granularity.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  logical table corresponding to the table name specified in the URL
     * @param logicalTableDictionary  Logical table dictionary contains the map of valid table names and table objects.
     *
     * @return Set of logical table objects.
     * @throws BadApiRequestException Invalid table exception if the table dictionary returns a null.
     */
    protected LogicalTable generateTable(
            String tableName,
            Granularity granularity,
            LogicalTableDictionary logicalTableDictionary
    ) throws BadApiRequestException {
        LogicalTable generated = logicalTableDictionary.get(new TableIdentifier(tableName, granularity));

        // check if requested logical table grain pair exists
        if (generated == null) {
            String msg = TABLE_GRANULARITY_MISMATCH.logFormat(granularity, tableName);
            LOG.error(msg);
            throw new BadApiRequestException(msg);
        }

        LOG.trace("Generated logical table: {} with granularity {}", generated, granularity);
        return generated;
    }

    @Override
    public Optional<String> getDownloadFilename() {
        return Optional.ofNullable(downloadFilename);
    }

    @Override
    public ResponseFormatType getFormat() {
        return format;
    }

    @Override
    public Optional<PaginationParameters> getPaginationParameters() {
        return paginationParameters;
    }

    @Override
    public Long getAsyncAfter() {
        return asyncAfter;
    }

    @Deprecated
    public PaginationParameters getDefaultPagination() {
        return DEFAULT_PAGINATION;
    }

    /**
     * This method returns a Function that can basically take a Collection and return an instance of
     * AllPagesPagination.
     *
     * @param paginationParameters  The PaginationParameters to be used to generate AllPagesPagination instance
     * @param <T>  The type of items in the Collection which needs to be paginated
     *
     * @return A Function that takes a Collection and returns an instance of AllPagesPagination
     */
    public <T> Function<Collection<T>, AllPagesPagination<T>> getAllPagesPaginationFactory(
            PaginationParameters paginationParameters
    ) {
        return data -> new AllPagesPagination<>(data, paginationParameters);
    }

    /**
     * Parses the asyncAfter parameter into a long describing how long the user is willing to wait for the results of a
     * synchronous request before the request should become asynchronous.
     *
     * @param asyncAfterString  asyncAfter should be either a string representation of a long, or the String never
     *
     * @return A long describing how long the user is willing to wait
     *
     * @throws BadApiRequestException if asyncAfterString is neither the string representation of a natural number, nor
     * {@code never}
     */
    protected long generateAsyncAfter(String asyncAfterString) throws BadApiRequestException {
        try {
            return asyncAfterString.equals(SYNCHRONOUS_REQUEST_FLAG) ?
                    SYNCHRONOUS_ASYNC_AFTER_VALUE :
                    asyncAfterString.equals(ASYNCHRONOUS_REQUEST_FLAG) ?
                            ASYNCHRONOUS_ASYNC_AFTER_VALUE :
                            Long.parseLong(asyncAfterString);
        } catch (NumberFormatException e) {
            LOG.debug(INVALID_ASYNC_AFTER.logFormat(asyncAfterString), e);
            throw new BadApiRequestException(INVALID_ASYNC_AFTER.format(asyncAfterString), e);
        }
    }
}
