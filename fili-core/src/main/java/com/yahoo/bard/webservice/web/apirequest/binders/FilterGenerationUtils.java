package com.yahoo.bard.webservice.web.apirequest.binders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_DIMENSION_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_ERROR;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_FIELD_NOT_IN_DIMENSIONS;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_OPERATOR_INVALID;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.util.FilterTokenizer;
import com.yahoo.bard.webservice.util.Incubating;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.BadFilterException;
import com.yahoo.bard.webservice.web.DefaultFilterOperation;
import com.yahoo.bard.webservice.web.FilterOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

@Incubating
public final class FilterGenerationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FilterGenerationUtils.class);

    /**
     * Common patterns used for parsing api filter queries.
     */
    public static Pattern COMMA_AFTER_BRACKET_PATTERN = Pattern.compile("(?<=]),");
    public static Pattern API_FILTER_PATTERN = Pattern.compile("([^\\|]+)\\|([^-]+)-([^\\[]+)\\[([^\\]]+)\\]?");

    /**
     * Default {@link FilterFactory}. FilterFactory constructs {@link ApiFilter} out of {@link FilterComponents}.
     */
    public static FilterFactory DEFAULT_FILTER_FACTORY = new FilterFactory();


    /**
     * Private constructor. This is a non-stantiable util class.
     */
    private FilterGenerationUtils() {
        throw new AssertionError("FitlerGenerationUtils is a non-stantiable util class");
    }

    /**
     * Data object to wrap string models.
     */
    public static class FilterDefinition {
        protected String dimensionName;
        protected String fieldName;
        protected String operationName;
        protected List<String> values;
    }

    /**
     * Data object for collecting the bound arguments needed to build an ApiFilter.
     */
    public static class FilterComponents {
        public final Dimension dimension;
        public final DimensionField dimensionField;
        public final FilterOperation operation;
        public final List<String> values;

        /**
         * Constructor.
         *
         * @param dimension  Dimension for ApiFilter.
         * @param field  Field for ApiFilter.
         * @param operation  Operation for ApiFilter.
         * @param values  Values for ApiFilters.
         */
        public FilterComponents(
                Dimension dimension,
                DimensionField field,
                FilterOperation operation,
                List<String> values
        ) {
            this.dimension = dimension;
            this.dimensionField = field;
            this.operation = operation;
            this.values = values;
        }
    }

    /**
     * Utility method to parse an api filter query and capture the unbound parameters for binding and validating
     * filters.
     *
     * @param filterQuery  The raw filterQuery as provided by the URI.
     *
     * @return  An object describing the string components from the request URI.
     */
    public static FilterDefinition buildFilterDefinition(String filterQuery) {
        FilterDefinition filterDefinition = new FilterDefinition();

        Matcher matcher = API_FILTER_PATTERN.matcher(filterQuery);

        if (!matcher.matches()) {
            throw new BadFilterException(FILTER_INVALID.format(filterQuery));
        }

        filterDefinition.dimensionName = matcher.group(1);
        filterDefinition.fieldName = matcher.group(2);
        filterDefinition.operationName = matcher.group(3);
        // replaceAll takes care of any leading ['s or trailing ]'s which might mess up this.values
        List<String> values = new ArrayList<>(
                FilterTokenizer.split(
                        matcher.group(4)
                                .replaceAll("\\[", "")
                                .replaceAll("\\]", "")
                                .trim()
                )
        );
        filterDefinition.values = values;
        return filterDefinition;
    }

    public static FilterComponents generateFilterComponents(
            @NotNull String filterQuery,
            DimensionDictionary dimensionDictionary
    ) {
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
        Dimension dimension;
        DimensionField dimensionField;
        FilterOperation operation;
        FilterDefinition definition;

        try {
            definition = FilterGenerationUtils.buildFilterDefinition(filterQuery);

            // Extract filter dimension form the filter query.
            dimension = dimensionDictionary.findByApiName(definition.dimensionName);

            // If no filter dimension is found in dimension dictionary throw exception.
            if (dimension == null) {
                LOG.debug(FILTER_DIMENSION_UNDEFINED.logFormat(definition.dimensionName));
                throw new BadFilterException(FILTER_DIMENSION_UNDEFINED.format(definition.dimensionName));
            }

            try {
                dimensionField = dimension.getFieldByName(definition.fieldName);
            } catch (IllegalArgumentException ignored) {
                LOG.debug(FILTER_FIELD_NOT_IN_DIMENSIONS.logFormat(definition.fieldName, definition.dimensionName));
                throw new BadFilterException(
                        FILTER_FIELD_NOT_IN_DIMENSIONS.format(definition.fieldName, definition.dimensionName)
                );
            }

            try {
                operation = DefaultFilterOperation.fromString(definition.operationName);
            } catch (IllegalArgumentException ignored) {
                LOG.debug(FILTER_OPERATOR_INVALID.logFormat(definition.operationName));
                throw new BadFilterException(FILTER_OPERATOR_INVALID.format(definition.operationName));
            }


        } catch (IllegalArgumentException e) {
            LOG.debug(FILTER_ERROR.logFormat(filterQuery, e.getMessage()), e);
            throw new BadFilterException(FILTER_ERROR.format(filterQuery, e.getMessage()), e);
        }
        return new FilterComponents(dimension, dimensionField, operation, definition.values);
    }

    /**
     * Parses the URL filter Query and generates the ApiFilter object.
     *
     * @param filterQuery  Expects a URL filter query String in the format:
     * <code>(dimension name)|(field name)-(operation)[?(value or comma separated values)]?</code>
     * @param dimensionDictionary  cache containing all the valid dimension objects.
     *
     * @return the ApiFilter
     */
    public static ApiFilter generateApiFilter(
            @NotNull String filterQuery,
            DimensionDictionary dimensionDictionary
    ) {
        FilterComponents components = FilterGenerationUtils.generateFilterComponents(filterQuery, dimensionDictionary);
        return FilterGenerationUtils.DEFAULT_FILTER_FACTORY.buildFilter(
                components.dimension,
                components.dimensionField,
                components.operation,
                components.values
        );
    }

    /**
     * Utility method to take two Api filters which differ only by value sets and union their value sets.
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
