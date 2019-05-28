// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.filterbuilders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_OPERATOR_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TOO_MANY_DRUID_FILTERS;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.FilterBuilderException;
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder;
import com.yahoo.bard.webservice.druid.model.filter.AndFilter;
import com.yahoo.bard.webservice.druid.model.filter.BoundFilter;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.exception.TooManyDruidFiltersException;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.DefaultFilterOperation;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.FilterOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A DruidBoundFilterBuilder builds a Druid Bound Filter for supported dimensions, which can be sent directly to Druid.
 * <p>
 *     Druid Bound Filter currently supports the following API filter operations:
 *     <ul>
 *          <li>{@link DefaultFilterOperation#gt}</li>
 *          <li>{@link DefaultFilterOperation#lt}</li>
 *          <li>{@link DefaultFilterOperation#gte}</li>
 *          <li>{@link DefaultFilterOperation#lte}</li>
 *          <li>{@link DefaultFilterOperation#between}</li>
 *     </ul>
 *
 * <p>
 *     So the filter:
 *     {@code startDate|id-gt[2018-10-10]}
 *     is translated into
 *     {@code DruidBoundFilterBuilder(startDate,2018-10-10,null)}
 *     which is serialized as following to druid
 *     <pre><code>
 *         {
 *             "type": "bound",
 *             "dimension": "startDate",
 *             "lower": "2018-10-10",
 *             "lowerStrict": false,
 *             "upper": null ,
 *             "upperStrict": false,
 *             "ordering": null }
 *         }
 *     </code></pre>
 *
 */
public class DruidBoundFilterBuilder implements DruidFilterBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(DruidBoundFilterBuilder.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final int DEFAULT_MAX_NUM_DRUID_FILTERS = 10000;
    private static final int MAX_NUM_DRUID_FILTERS = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("max_num_druid_filters"),
            DEFAULT_MAX_NUM_DRUID_FILTERS
    );

    @Override
    public Filter buildFilters(final Map<Dimension, Set<ApiFilter>> filterMap) throws FilterBuilderException {
        LOG.trace("Building Druid Bound Filters using filter map: {}", filterMap);

        if (filterMap.isEmpty()) {
            return null;
        }

        List<Filter> filters;
        try {
            filters =
                    filterMap.values()
                            .stream()
                            .flatMap(Set::stream)
                            // Normalize allows us to expand one filter to many, or prune filters from processing
                            // without a failure
                            .flatMap(this::normalize)
                            .peek(this::validateFilter)
                            .map(this::buildDruidBoundFilters)
                            .collect(Collectors.toList());
        } catch (IllegalArgumentException e) {
            if (e.getCause() instanceof FilterBuilderException) {
                throw (FilterBuilderException) e.getCause();
            }
            throw e;
        }

        if (filters.isEmpty()) {
            return null;
        }

        if (filters.size() == 1) {
            return filters.get(0);
        }

        AndFilter boundFilter = new AndFilter(filters);

        if (boundFilter.getFields().size() > MAX_NUM_DRUID_FILTERS) {
            LOG.error(TOO_MANY_DRUID_FILTERS.logFormat());
            throw new TooManyDruidFiltersException(TOO_MANY_DRUID_FILTERS.format());
        }

        LOG.trace("Filter: {}", boundFilter);
        return boundFilter;
    }

    /**
     * Builds a Druid Bound Filter based on the operation type.
     *
     * @param filter APIFilter associated to a given dimension
     *
     * @return A Druid Bound Filter based on the filter operation type
     */
    public Filter buildDruidBoundFilters(ApiFilter filter) {
        List<String> values = filter.getValuesList();
        Dimension dimension = filter.getDimension();
        DefaultFilterOperation filterOperation = (DefaultFilterOperation) filter.getOperation();
        switch (filterOperation) {
            case lte:
                return BoundFilter.buildUpperBoundFilter(dimension, values.get(0), true);
            case gte:
                return BoundFilter.buildLowerBoundFilter(dimension, values.get(0), true);
            case gt:
                return BoundFilter.buildLowerBoundFilter(dimension, values.get(0), false);
            case lt:
                return BoundFilter.buildUpperBoundFilter(dimension, values.get(0), false);
            case between:
                String lowerBound = values.get(0);
                String upperBound = values.get(1);
                return new BoundFilter(dimension, lowerBound, upperBound, false, true, null);
            default:
                LOG.error(FILTER_OPERATOR_INVALID.logFormat(filterOperation.getName()));
                throw new IllegalArgumentException(
                        new FilterBuilderException(FILTER_OPERATOR_INVALID.format(filterOperation.getName()))
                );
        }
    }

    /**
     * Validate the expected values for this Filter.
     *
     * This is useful for clean error messaging on filters as well as to clean up Filters before conversion.
     *
     * @param filter  the api filter being validated
     *
     */
    public void validateFilter(ApiFilter filter) {
        FilterOperation op = filter.getOperation();
        List<String> values = filter.getValuesList();

        // Verify that this is a valid operation for this builder
        if (! (op instanceof DefaultFilterOperation)) {
            LOG.error(FILTER_OPERATOR_INVALID.logFormat(op));
            throw new IllegalArgumentException(
                    new FilterBuilderException(FILTER_OPERATOR_INVALID.format(op.getName()))
            );
        }

        // Verify that this filter uses a correct number of filters
        if (
                (op.getMinimumArguments().isPresent() && op.getMinimumArguments().get() < values.size()) ||
                (op.getMaximumArguments().isPresent() && op.getMaximumArguments().get() > values.size())
        ) {
            String error = ErrorMessageFormat.FILTER_WRONG_NUMBER_OF_VALUES.format(
                    op,
                    op.expectedRangeDescription(),
                    filter.getValues().size(),
                    filter.getValuesList()
            );
            LOG.error(error);
            throw new IllegalArgumentException(new FilterBuilderException(error));
        }


    }

    /**
     * An extension hook to permit subclasses to transform, remove, or make multiple versions of an apiFilter.
     *
     * @param filter  The filter to transform
     *
     * @return the transformed filter
     */
    public Stream<ApiFilter> normalize(ApiFilter filter) {
        return Stream.of(filter);
    }
}
