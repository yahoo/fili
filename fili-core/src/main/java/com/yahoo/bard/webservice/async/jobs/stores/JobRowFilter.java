// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.stores;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_ERROR;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_JOBFIELD_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_OPERATOR_INVALID;

import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField;
import com.yahoo.bard.webservice.async.jobs.jobrows.JobField;
import com.yahoo.bard.webservice.util.FilterTokenizer;
import com.yahoo.bard.webservice.web.BadFilterException;
import com.yahoo.bard.webservice.web.FilterOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

/**
 * Class containing filter information to filter JobRows in ApiJobStore.
 */
public class JobRowFilter {
    private static final Logger LOG = LoggerFactory.getLogger(JobRowFilter.class);

    private final JobField jobField;
    private final FilterOperation operation;
    private final Set<String> values;

    /*  url filter query pattern:  (JobField name)-(operation)[(value or comma separated numeric values)]?
    *
    *  e.g.    userId-eq[Foo]
    *
    *          JobField name:    userId
    *          operation:        eq
    *          values:           Foo
    */
    private static final Pattern QUERY_PATTERN = Pattern.compile("([^\\|]+)-([^\\[]+)\\[([^\\]]+)\\]?");

    /**
     * Parses the URL ApiJobStore filter query and generates the JobRowFilter object.
     *
     * @param filterQuery  Expects a URL ApiJobStore filter query String in the format:
     * <p>
     * <code>(JobField name)-(operation)[?(value or comma separated values)]?</code>
     *
     * @throws BadFilterException when filter pattern is not matched or when any of its properties are not
     * valid.
     */
    public JobRowFilter(@NotNull String filterQuery) throws BadFilterException {
        LOG.trace("filterQuery: {}", filterQuery);

        Matcher tokenizedQuery = QUERY_PATTERN.matcher(filterQuery);

        // if pattern match found, extract values else throw exception
        if (!tokenizedQuery.matches()) {
            LOG.debug(FILTER_INVALID.logFormat(filterQuery));
            throw new BadFilterException(FILTER_INVALID.format(filterQuery));
        }

        jobField = extractJobField(tokenizedQuery);
        operation = extractOperation(tokenizedQuery);
        values = extractValues(tokenizedQuery, filterQuery);
    }

    /**
     * Constructor for an JobRowFilter object whose data has already been parsed.
     *
     * @param jobField  The JobField to perform the filtering on
     * @param operation  The operation to perform (eg: eq)
     * @param values  A Set of Strings to compare the JobField's value to.
     */
    private JobRowFilter(JobField jobField, FilterOperation operation, Set<String> values) {
        this.jobField = jobField;
        this.operation = operation;
        this.values = values;
    }

    public JobField getJobField() {
        return jobField;
    }

    public FilterOperation getOperation() {
        return operation;
    }

    public Set<String> getValues() {
        return values;
    }

    /**
     * Construct an JobRowFilter object using the same FilterOperation and values as the object on
     * which this method is called and using the supplied JobField.
     *
     * @param jobField  The JobField to perform the filtering on
     *
     * @return An instance of JobRowFilter created using the supplied JobField
     */
    public JobRowFilter withJobField(JobField jobField) {
        return new JobRowFilter(jobField, operation, values);
    }

    /**
     * Construct an JobRowFilter object using the same JobField and values as the object on
     * which this method is called and using the supplied FilterOperation.
     *
     * @param operation  The operation to perform (eg: eq)
     *
     * @return An instance of JobRowFilter created using the supplied FilterOperation
     */
    public JobRowFilter withOperation(FilterOperation operation) {
        return new JobRowFilter(jobField, operation, values);
    }

    /**
     * Construct an JobRowFilter object using the same JobField and FilterOperation as the object on
     * which this method is called and using the supplied values.
     *
     * @param values  A Set of Strings to compare the JobField's value to
     *
     * @return  An instance of JobRowFilter created using the supplied values
     */
    public JobRowFilter withValues(Set<String> values) {
        return new JobRowFilter(jobField, operation, values);
    }

    /**
     * Extracts the JobField to be examined from the tokenizedQuery.
     *
     * @param tokenizedQuery  The tokenized filter expression.
     *
     * @return  The JobField to be examined
     * @throws BadFilterException is the JobField does not exist
     */
    private JobField extractJobField(Matcher tokenizedQuery) throws BadFilterException {
        String fieldName = tokenizedQuery.group(1);

        return Arrays.stream(DefaultJobField.values())
                .filter(field -> field.getName().equals(fieldName))
                .findFirst()
                .orElseThrow(() -> {
                            LOG.debug(FILTER_JOBFIELD_UNDEFINED.logFormat(fieldName, DefaultJobField.values()));
                            return new BadFilterException(FILTER_JOBFIELD_UNDEFINED.format(
                                    fieldName, DefaultJobField.values()
                            ));
                });
    }

    /**
     * Extracts the operation to be performed by the ApiJobStore filter query.
     *
     * @param tokenizedQuery  The tokenized filter expression.
     *
     * @return The operation to be performed by the ApiJobStore filter query.
     * @throws BadFilterException if the operation does not exist
     */
    private FilterOperation extractOperation(Matcher tokenizedQuery) throws BadFilterException {
        String operationName = tokenizedQuery.group(2);
        try {
            return FilterOperation.valueOf(operationName);
        } catch (IllegalArgumentException ignored) {
            LOG.debug(FILTER_OPERATOR_INVALID.logFormat(operationName));
            throw new BadFilterException(FILTER_OPERATOR_INVALID.format(operationName));
        }
    }

    /**
     * Extracts the values to be used in the JobRowFilter query from the query.
     *
     * @param tokenizedQuery  The tokenized filter expression..
     * @param filterQuery  The raw query. Used for logging.
     *
     * @return The set of values to be used in the JobRowFilter query.
     * @throws BadFilterException If the fragment of the query that specifies the values is malformed.
     */
    private Set<String> extractValues(Matcher tokenizedQuery, String filterQuery) throws BadFilterException {
        try {
            // replaceAll takes care of any leading ['s or trailing ]'s which might mess up the values set.
            return new LinkedHashSet<>(
                    FilterTokenizer.split(
                            tokenizedQuery.group(3)
                                    .replaceAll("\\[", "")
                                    .replaceAll("\\]", "")
                                    .trim()
                    )
            );
        } catch (IllegalArgumentException e) {
            LOG.debug(FILTER_ERROR.logFormat(filterQuery, e.getMessage()), e);
            throw new BadFilterException(FILTER_ERROR.format(filterQuery, e.getMessage()), e);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (!(o instanceof JobRowFilter)) { return false; }

        JobRowFilter jobRowFilter = (JobRowFilter) o;

        return
                Objects.equals(jobField, jobRowFilter.jobField) &&
                        Objects.equals(operation, jobRowFilter.operation) &&
                        Objects.equals(values, jobRowFilter.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobField, operation, values);
    }
}
