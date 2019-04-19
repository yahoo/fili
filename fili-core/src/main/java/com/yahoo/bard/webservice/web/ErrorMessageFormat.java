// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.MessageFormatter;

/**
 * Common message formats for errors.
 */
public enum ErrorMessageFormat implements MessageFormatter {

    TABLE_UNDEFINED("Table name '%s' does not exist."),
    TABLE_ALIGNMENT_UNDEFINED("Table '%s' cannot be aligned to a request with intervals: %s."),
    TABLE_SCHEMA_UNDEFINED(
            "Table '%s' is incompatible with the dimensions '%s', metrics '%s' and granularity '%s' requested.",
            "No PhysicalTable for Logical Table '%s' with the availability-based dimensions '%s', " +
                    "metrics '%s', and granularity '%s' found "
    ),
    TABLE_SCHEMA_CONSTRAINT_MISMATCH(
            "Received invalid request requesting for column(s): %s that are not available in this table: %s"
    ),

    EMPTY_DICTIONARY("%s Dictionary is empty."),

    DIMENSIONS_UNDEFINED("Dimension(s) '%s' do not exist."),
    DIMENSIONS_NOT_IN_TABLE("Requested dimension(s) '%s' are not supported by the table '%s'."),
    DIMENSION_FIELDS_UNDEFINED("Dimension field(s) '%s' do not exist for dimension '%s'"),
    DIMENSION_TYPE_INVALID("The dimension type '%s' for dimension '%s' is invalid"),

    SLICE_UNDEFINED("Slice name '%s' does not exist."),

    UNKNOWN_GRANULARITY("'%s' is not a valid granularity. Try 'day', 'week', 'month', 'year' or 'all'"),
    GRANULARITY_MERGE("'%s' time zone cannot be applied to time grain '%s'"),

    INVALID_GRANULARITY("Granularity %s is of an unexpected type %s."),
    TABLE_GRANULARITY_MISMATCH("Invalid pair of granularity '%s' and table '%s'."),

    TIME_ALIGNMENT("'%s' does not align with granularity '%s'.%s"),
    EMPTY_INTERVAL_FORMAT("Query has an interval of zero duration: %s"),

    QUERY_GRAIN_NOT_SATISFIED(
            "Illegal request: query requires time grain '%s' which cannot satisfy request time grain '%s'."
    ),

    INTERVAL_MISSING("Required parameter dateTime is missing. Use dateTime=YYYY-MM-DD/YYYY-MM-DD in the query string."),
    INTERVAL_INVALID("Interval '%s' is invalid. %s."),
    INVALID_INTERVAL_GRANULARITY("Invalid macro %s with the date interval %s"),
    INTERVAL_ZERO_LENGTH("Date time cannot have zero length intervals. %s."),

    INVALID_TIME_ZONE(
            "Time zone '%s' is unknown.  See (http://joda-time.sourceforge.net/timezones.html) for valid time zone ids."
    ),

    METRICS_MISSING(
            "Required parameter metrics is missing or empty. Use 'metrics=METRICNAME1,METRICNAME2' in the query string."
    ),
    METRICS_UNDEFINED("Metric(s) '%s' do not exist."),
    METRICS_NOT_IN_TABLE("Requested metric(s) '%s' are not supported by the table '%s'."),
    DUPLICATE_METRICS_IN_API_REQUEST(
            "Duplicate metric(s) are not allowed in the API request even if one is filtered and the other is " +
                    "unfiltered. Duplicate metric(s) are '%s'."
    ),
    INCORRECT_METRIC_FILTER_FORMAT(
            "The format of the metric filter is incorrect '%s'. The format should be " +
                    "like metrics=metric1,metric2(AND(dim1|id-in[value1,value2],dim2|id-in[value3,value4]))"
    ),
    INVALID_METRIC_FILTER_CONDITION("Filter condition '%s' is not supported"),
    UNSUPPORTED_FILTERED_METRIC_CATEGORY(
            "Metric filtering is not supported for metric '%s' as it belongs to %s' category"
    ),

    INVALID_NUMBER_OF_FIELDS("fields length shouldn't be more then one for Sketch Estimate operation %s"),

    SORT_DIRECTION_INVALID("Sort direction '%s' is invalid."),
    SORT_METRICS_NOT_IN_QUERY_FORMAT("Requested sort metric(s) '%s' were not selected in the metrics expression."),
    SORT_METRICS_NOT_SORTABLE_FORMAT("Sorting not possible on metric(s) '%s'."),
    SORT_METRICS_UNDEFINED("Metric(s) in sort expression '%s' do not exist."),

    DATE_TIME_SORT_VALUE_INVALID("dateTime should be the first field in the sort list"),

    ACCEPT_FORMAT_INVALID("Format '%s' is unknown. Choose from 'csv', 'json'."),

    FILTER_INVALID("Filter expression '%s' is invalid."),
    FILTER_ERROR("Filter expression '%s' resulted in the following error: %s."),
    FILTER_DIMENSION_UNDEFINED("Filter dimension '%s' does not exist."),
    FILTER_DIMENSION_NOT_IN_TABLE("Filter dimension '%s' is not supported by the table '%s'."),
    FILTER_FIELD_NOT_IN_DIMENSIONS("Filter dimension field '%s' is not supported by the dimension '%s'."),
    FILTER_OPERATOR_INVALID("Filter operator '%s' is invalid."),
    FILTER_WRONG_NUMBER_OF_VALUES("Filter operator '%s' expects %s argument(s).  Found %d in '%s'."),
    FILTER_SUBSTRING_OPERATIONS_DISABLED(
            "Filter operations 'startswith' and 'contains' are disabled for data requests.",
            "Filter operations 'startswith' and 'contains' are disabled for data requests. Enable by setting feature" +
                    "flag: data_starts_with_contains_enabled"
    ),
    FILTER_DIMENSION_MISMATCH("Filter dimension %s does not match dimension %s."),


    INTEGER_INVALID("%s value:'%s' is invalid. Value must be a positive integer."),

    TOP_N_UNSORTED(
            "TopN requires at least one metric column to sort on, specified using a query parameter of the " +
                    "form 'sort=metricName|dir' where 'dir' is one of 'asc' (ascending) or 'desc' (descending)"
    ),

    PAGINATION_PARAMETER_MISSING("Missing parameter '%s.' Both 'perPage' and 'page' are required for pagination."),
    PAGINATION_PARAMETER_INVALID("Parameter '%s' expected a positive integer but received: '%s'"),
    PAGINATION_PAGE_INVALID("Requested page '%d' with '%d' rows per page, but there are only '%d' pages."),

    NO_PRERESPONSE("Pre response is not available to publish the response for this request"),

    RESULT_SET_ERROR("Cannot build result set for query of type: %s."),
    TOO_MANY_PERIODS("Too many periods between the epoch %s and the target %s to calculate alignment on grain %s."),

    HAVING_INVALID("Having expression '%s' is invalid."),
    HAVING_NON_NUMERIC("Having expression '%s' is not numeric."),
    HAVING_ERROR("Having expression '%s' resulted in the following error: %s."),
    HAVING_METRIC_UNDEFINED("Having metric '%s' does not exist."),
    HAVING_METRIC_NOT_IN_TABLE("Having metric '%s' is not supported by the table '%s'."),
    HAVING_METRICS_NOT_IN_QUERY_FORMAT("Requested having metric(s) '%s' were not selected in the metrics expression."),
    HAVING_OPERATOR_INVALID("Having operator '%s' is invalid."),

    LOGINFO_CLASS_INVALID("Invalid LogInfo class: %s. Cannot define its order. Ignoring."),

    DRUID_METADATA_READ_ERROR("Unable to read metadata for: '%s'."),
    DRUID_METADATA_SEGMENTS_MISSING("No segment metadata available for data sources: '%s'."),

    DRUID_URL_INVALID("Druid %s url is unset."),

    WEIGHT_CHECK_FAILED(
            "Result set too large. Try reducing interval, dimensions, or sketch metrics.",
            "The product of sketches and rows is too large: %d > %d"
    ),

    NON_AGGREGATABLE_INVALID("Query contains invalid use of the non-aggregatable dimensions: %s"),
    NO_TABLE_FOR_NON_AGGREGATABLE(
            "No table supports aggregation to exactly non-aggregatable dimensions: %s and aggregatable dimensions: %s"
    ),

    NO_PHYSICAL_TABLE_MATCHED(
            "No matching physical table found for dimensions '%s', metrics '%s', and time grain '%s'"
    ),

    DIMENSION_ROWS_NOT_FOUND("Dimension rows not found for %s with filter %s"),

    UNABLE_TO_FIND_DIMENSION_FROM_DICTIONARY("Unable to find dimension from Dimension dictionary for the name: %s"),
    METRIC_VALUE_PARSING_ERROR("Unable to %s metric value and its type"),
    GRANULARITY_PARSING_ERROR("No granularity can be parsed from this name: %s"),
    UNKNOWN_TIMEZONE_ID("Unable to recognize the timeZoneId: %s"),
    UNABLE_TO_SERIALIZE("Unable to serialize the %s object"),
    METRIC_VALUE_CLASS_NOT_FOUND("Metric value type class %s is not found"),

    UNAUTHORIZED_WITH_RETRY("Not authorized to access the resource for given id %s. Retrying again %s more times"),
    UNAUTHORIZED("Not authorized to access the resource for given id %s."),
    RESOURCE_NOT_FOUND("Resource for the given id %s does not exist"),

    MSG_UNABLE_TO_DESERIALIZE(
            "Encountered an error while retrieving the results for job id %s. The data may have been corrupted."
    ),
    LOG_UNABLE_TO_DESERIALIZE("Unable to deserialize results for job %s from %s"),

    RESOURCE_RETRIEVAL_FAILURE("Unable to retrieve the resource for given resource name: %s. Info: %s"),
    RESOURCE_STORAGE_FAILURE("Unable to store the resource for resource name %s. %s"),
    RESOURCE_DELETION_FAILURE("Unable to delete the resource for resource name %s. %s"),

    UNSUPPORTED_LOOKBACKQUERY_OPERATION(
            "LookbackQuery creation failed for the requested metric",
            "withPostAggregations() is not supported by LookbackQuery. Try using withInnerQueryPostAggregations or " +
                    "withLookbackQueryPostAggregations"
    ),

    JOB_NOT_FOUND("No job found with job ticket %s"),

    JOB_MAPPING_FAILED(
            "Job with ticket %s cannot be retrieved due to internal error",
            "The JobRow %s could not be correctly mapped to a job payload by the JobPayloadBuilder because the " +
                    "fields %s are missing in the job row "
    ),

    JOBS_RETREIVAL_FAILED(
            "Jobs cannot be retrieved successfully due to internal error",
            "Jobs cannot be retrieved successfully as the JobRow %s could not be correctly mapped to a job payload " +
                    "by the JobPayloadBuilder"
    ),

    PHYSICAL_NAME_NOT_FOUND(
            "Could not resolve physical name to serialize dimension.",
            "Could not resolve physical name to serialize dimension for name: %s"
    ),

    EITHER_ERROR_LEFT_OF_RIGHT(
            "Bard experienced an internal error. Sorry.",
            "Attempted to get the Left value of a Right Either: %s"
    ),

    EITHER_ERROR_RIGHT_OF_LEFT(
            "Bard experienced an internal error. Sorry.",
            "Attempted to get the Right value of a Left Either: %s"
    ),

    FAILED_TO_PUBLISH_ERROR("Failed to publish error Response."),

    FAILED_TO_SAVE_JOB_ROW("Error encountered. %s not saved to ApiJobStore"),

    MISSING_JOB_ID("Bard experienced an internal error. Sorry.", "Missing id for job row %s"),

    INVALID_ASYNC_AFTER(
            "Invalid 'asyncAfter' parameter: '%s'. 'asyncAfter' must be either 'never' or an " +
                    "integer number of milliseconds."
    ),

    FILTER_JOBFIELD_UNDEFINED("Filter field '%s' does not exist. The possible fields to filter on are '%s'"),

    FAILED_TO_SEND_QUERY_TO_DRUID("Failed to retrieve data.", "Failed to send the query %s to Druid."),

    ERROR_FROM_DRUID(
            "Failed to retrieve data.",
            "Received %s with status code %s for reason %s when sending %s to Druid"
    ),

    RESULT_MAPPING_FAILURE(
            "Error occurred while processing response data: %s"
    ),

    INVALID_DATASOURCE_UNION(
            "Union Data Source had conflicting name mappings for logical dimension '%s' with mappings of '%s' and '%s'"
    ),

    DATA_AVAILABILITY_MISMATCH(
            "Data availability expectation does not match with actual query result obtained from druid for the " +
                    "following intervals %s where druid does not have data"
    ),

    TOO_MANY_INTERVALS_MISSING(
            "Query is returning more than the configured limit of '%s' missing intervals. " +
                    "There may be a problem with your data."
    ),

    CONTEXT_AND_STATUS_MISSING_FROM_RESPONSE("JSON response is missing X-Druid-Response-Context and status code"),

    DRUID_RESPONSE_CONTEXT_MISSING_FROM_RESPONSE("JSON response is missing X-Druid-Response-Context"),

    UNCOVERED_INTERVALS_MISSING_FROM_RESPONSE(
            "JSON response is missing 'uncoveredIntervals' from X-Druid-Response-Context header"
    ),

    UNCOVERED_INTERVALS_OVERFLOWED_MISSING_FROM_RESPONSE(
            "JSON response is missing 'uncoveredIntervalsOverflowed' from X-Druid-Response-Context header"
    ),

    STATUS_CODE_MISSING_FROM_RESPONSE("JSON response is missing response status code"),

    ETAG_MISSING_FROM_RESPONSE("JSON response is missing response etag"),

    INTERNAL_SERVER_ERROR_ON_JSON_PROCESSING("Internal server error. JsonProcessingException : %s"),

    INTERNAL_SERVER_ERROR_REASON_PHRASE(
            "The server encountered an unexpected condition which prevented it from fulfilling the request."
    ),

    TOO_MANY_BACKING_DATA_SOURCES("TableDataSource built with too many backing data sources: %s"),
    TOO_FEW_BACKING_DATA_SOURCES("TableDataSource built with insufficient backing data sources: %s"),
    HAVING_OPERATOR_WRONG_NUMBER_OF_PARAMETERS("Wrong number of parameters provided for %s operator. " +
                                              "%s operator needs %d parameters but found %d"),
    HAVING_OPERATOR_IMPROPER_RANGE("Upper range cannot be less than the lower range for %s operator."),

    TOO_MANY_DRUID_FILTERS(
            "Too many filtering dimension values found. Request is aborted because it will significantly slows down " +
                    "or timeout Druid query.",
            "DruidFilterBuilder generated too many filters. Request is aborted."
    ),

    UNABLE_TO_CREATE_DIR("Unable to create directory %s."),
    UNABLE_TO_DELETE_DIR("Unable to delete directory %s."),
    FAIL_TO_WIPTE_LUCENE_INDEX_DIR("Failed to wipte Lucene index at directory: %s"),

    REQUEST_PROCESSING_EXCEPTION("Exception processing request: %s"),

    TWO_VALUES_OF_THE_SAME_KEY("Values %s and %s are associated with the same key"),

    LUCENE_LOCK_TIMEOUT("Waiting for lock timed out in LuceneSearchProvider for dimension %s"),
    LUCENE_LOCK_INTERRUPTED("Waiting for lock was interrupted in LuceneSearchProvider for dimension %s")
    ;

    private final String messageFormat;
    private final String loggingFormat;

    /**
     * An error message formatter with the same message for logging and messaging.
     *
     * @param messageFormat The format string for logging and messaging
     */
    ErrorMessageFormat(String messageFormat) {
        this(messageFormat, messageFormat);
    }

    /**
     * An error message formatter with different messages for logging and messaging.
     *
     * @param messageFormat The format string for messaging
     * @param loggingFormat The format string for logging
     */
    ErrorMessageFormat(String messageFormat, String loggingFormat) {
        this.messageFormat = messageFormat;
        this.loggingFormat = loggingFormat;
    }

    @Override
    public String getMessageFormat() {
        return messageFormat;
    }

    @Override
    public String getLoggingFormat() {
        return loggingFormat;
    }
}
