package com.yahoo.bard.webservice.web;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_DIMENSION_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_ERROR;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_FIELD_NOT_IN_DIMENSIONS;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_OPERATOR_INVALID;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.util.FilterTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 * Factory for ApiFilter objects. Has methods to build an ApiFilter object out of the filter clause in a Fili Api
 * request, and producing an ApiFilter by unioning the values of two provided ApiFilters.
 */
public class ApiFilterGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(ApiFilterGenerator.class);

    private static Pattern pattern = Pattern.compile("([^\\|]+)\\|([^-]+)-([^\\[]+)\\[([^\\]]+)\\]?");

    /**
     * Parses the URL filter Query and generates the ApiFilter object.
     *
     * @param filterQuery  Expects a URL filter query String in the format:
     * <p>
     * <code>(dimension name)|(field name)-(operation)[?(value or comma separated values)]?</code>
     *
     * @param dimensionDictionary  cache containing all the valid dimension objects.
     *
     * @throws BadFilterException Exception when filter pattern is not matched or when any of its properties are not
     * valid.
     * @return the ApiFilter
     */
    public static ApiFilter build(
            @NotNull String filterQuery,
            DimensionDictionary dimensionDictionary
    ) throws BadFilterException {
        LOG.trace("Filter query: {}\n\n DimensionDictionary: {}", filterQuery, dimensionDictionary);

        /*  url filter query pattern:  (dimension name)|(field name)-(operation)[?(value or comma separated values)]?
         *
         *  e.g.    locale|name-in[US,India]
         *          locale|id-eq[5]
         *
         *          dimension name: locale      locale
         *          field name:     name        id
         *          operation:      in          eq
         *          values:         US,India    5
         */
        ApiFilter inProgressApiFilter = new ApiFilter(null, null, null, new HashSet<>());

        Matcher matcher = pattern.matcher(filterQuery);

        // if pattern match found, extract values else throw exception
        if (!matcher.matches()) {
            LOG.debug(FILTER_INVALID.logFormat(filterQuery));
            throw new BadFilterException(FILTER_INVALID.format(filterQuery));
        }

        try {
            // Extract filter dimension form the filter query.
            String filterDimensionName = matcher.group(1);
            Dimension dimension = dimensionDictionary.findByApiName(filterDimensionName);

            // If no filter dimension is found in dimension dictionary throw exception.
            if (dimension == null) {
                LOG.debug(FILTER_DIMENSION_UNDEFINED.logFormat(filterDimensionName));
                throw new BadFilterException(FILTER_DIMENSION_UNDEFINED.format(filterDimensionName));
            }
            inProgressApiFilter = inProgressApiFilter.withDimension(dimension);

            String dimensionFieldName = matcher.group(2);
            try {
                DimensionField dimensionField = dimension.getFieldByName(dimensionFieldName);
                inProgressApiFilter = inProgressApiFilter.withDimensionField(dimensionField);
            } catch (IllegalArgumentException ignored) {
                LOG.debug(FILTER_FIELD_NOT_IN_DIMENSIONS.logFormat(dimensionFieldName, filterDimensionName));
                throw new BadFilterException(
                        FILTER_FIELD_NOT_IN_DIMENSIONS.format(dimensionFieldName, filterDimensionName)
                );
            }
            String operationName = matcher.group(3);
            try {
                FilterOperation operation = DefaultFilterOperation.valueOf(operationName);
                inProgressApiFilter = inProgressApiFilter.withOperation(operation);
            } catch (IllegalArgumentException ignored) {
                LOG.debug(FILTER_OPERATOR_INVALID.logFormat(operationName));
                throw new BadFilterException(FILTER_OPERATOR_INVALID.format(operationName));
            }

            // replaceAll takes care of any leading ['s or trailing ]'s which might mess up this.values
            Set<String> values = new LinkedHashSet<>(
                    FilterTokenizer.split(
                            matcher.group(4)
                                    .replaceAll("\\[", "")
                                    .replaceAll("\\]", "")
                                    .trim()
                    )
            );
            inProgressApiFilter = inProgressApiFilter.withValues(values);
        } catch (IllegalArgumentException e) {
            LOG.debug(FILTER_ERROR.logFormat(filterQuery, e.getMessage()), e);
            throw new BadFilterException(FILTER_ERROR.format(filterQuery, e.getMessage()), e);
        }
        return inProgressApiFilter;
    }

    /**
     * Take two Api filters which differ only by value sets and union their value sets.
     *
     * @param one  The first ApiFilter
     * @param two  The second ApiFilter
     *
     * @return an ApiFilter with the union of values
     */
    public static ApiFilter union(ApiFilter one, ApiFilter two) {
        if (Objects.equals(one.getDimension(), two.getDimension())
                && Objects.equals(one.getDimensionField(), two.getDimensionField())
                && Objects.equals(one.getOperation(), two.getOperation())
                ) {
            Set<String> values = Stream.concat(
                    one.getValues().stream(),
                    two.getValues().stream()
            )
                    .collect(Collectors.toSet());
            return one.withValues(values);
        }
        throw new IllegalArgumentException(String.format("Unmergable ApiFilters  '%s' and '%s'", one, two));
    }
}
