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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A DruidBoundFilterBuilder builds a Druid Bound Filter for supported dimensions, which can be sent directly to Druid.
 * <p>
 *     Druid Bound Filter currently supports the following API filter operations:
 *          <li>{@link DefaultFilterOperation#gt}</li>
 *          <li><{@link DefaultFilterOperation#lt}/li>
 *          <li>{@link DefaultFilterOperation#gte}</li>
 *          <li>{@link DefaultFilterOperation#lte}</li>
 *          <li>{@link DefaultFilterOperation#between}</li>
 * </p>
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
 * </p>
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

        List<Filter> filters;
        try {
            filters =
                    filterMap.values()
                            .stream()
                            .flatMap(Set::stream)
                            .map(filter -> buildDruidBoundFilters(filter))
                            .collect(Collectors.toList());
        } catch (RuntimeException e) {
            if (e.getCause() instanceof FilterBuilderException) {
                throw (FilterBuilderException) e.getCause();
            } else {
                throw e;
            }
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
                return BoundFilter
                        .buildUpperBoundFilter(dimension, values.get(1), false)
                        .withLowerBound(values.get(0));
            default:
                LOG.error(FILTER_OPERATOR_INVALID.logFormat(filterOperation.getName()));
                throw new RuntimeException(
                        new FilterBuilderException(FILTER_OPERATOR_INVALID.format(filterOperation.getName()))
                );
        }
    }
}
