// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_DIRECTION_INVALID;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.util.Incubating;
import com.yahoo.bard.webservice.web.BadApiRequestException;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Contract for generating the sorts for a data query. Sorts are only supported again the dateTime column in druid and
 * against metrics. Sorts are a combination of the metric to sort on and the direction for that sorting. Direction is
 * determined by the {@link SortDirection} enum. The data identifier to sort on is dependent on implementation. The
 * default sort generation strategy, implemented by {@link DefaultSortGenerator}, identifies metrics by their api names.
 * However, other implementations of SortGenerator are open to changing or expanding this functionality.
 */
@Incubating
public interface SortGenerator {

    /**
     * Name of the column in druid representing the date/time of a row of data.
     */
    String DATE_TIME_STRING = "dateTime";

    /**
     * Default implementation of {@link SortGenerator}.
     */
    SortGenerator DEFAULT_SORT_GENERATOR = new DefaultSortGenerator();

    /**
     * Utility method to parse the sort parameter from the api request into a mapping of data identifier to sort
     * direction.
     *
     * @param sorts  The raw sorts parameter from the api request.
     * @return the mapping of data identifier to sort direction.
     */
    static LinkedHashMap<String, SortDirection> generateSortDirectionMap(String sorts) {
        LinkedHashMap<String, SortDirection> sortDirectionMap = new LinkedHashMap<>();

        if (sorts != null && !sorts.isEmpty()) {
            Arrays.stream(sorts.split(","))
                    .map(e -> Arrays.asList(e.split("\\|")))
                    .forEach(e -> sortDirectionMap.put(e.get(0), getSortDirection(e)));
            return sortDirectionMap;
        } else {
            return null;
        }
    }

    /**
     * Utility method for resolving a pair of data identifier and sort direction from the api request sort parameter
     * into a {@link SortDirection}. If not sort direction is specified, {@link SortDirection#DESC} is returned.
     *
     * @param columnWithDirection  The combination of a data identifier and the string version of the metric.
     * @return the sort direction.
     * @incubating it is likely the contract on this method will be cleaned up to make it more suitable for a public
     * API.
     */
    @Incubating
    static SortDirection getSortDirection(List<String> columnWithDirection) {
        try {
            return columnWithDirection.size() == 2 ?
                    SortDirection.valueOf(columnWithDirection.get(1).toUpperCase(Locale.ENGLISH)) :
                    SortDirection.DESC;
        } catch (IllegalArgumentException ignored) {
            String sortDirectionName = columnWithDirection.get(1);
            throw new BadApiRequestException(SORT_DIRECTION_INVALID.format(sortDirectionName));
        }
    }

    /**
     * Generates a sort that includes dateTime from an already generated mapping of sort data identifier to sort
     * direction. A utility for generating this mapping is provided on this interface as
     * {@link SortGenerator#generateSortDirectionMap(String)}. Implementors looking to expand the sort capability of
     * Fili may need to build their own sortDirections mapping if the naive construction approach is not sufficient.
     *
     * @param sortDirections  The mapping of sortable data identifier to sort direction.
     * @return a date time based sort.
     */
    OrderByColumn generateDateTimeSort(LinkedHashMap<String, SortDirection> sortDirections);

    /**
     * Generates sorts on data identifiers that does NOT include sorting on date/time.
     *
     * @param sortDirections  The mapping of sort data identifier to sort direction. A utility for generating this
     * mapping is provided on this interface as {@link SortGenerator#generateSortDirectionMap(String)}. Implementors
     * looking to expand the sort capability of Fili may need to build their own sortDirections mapping if the naive
     * construction approach is not sufficient.
     * @param logicalMetrics  The set of logical metrics in the request.
     * @param metricDictionary  The metric dictionary.
     * @return the ORDERED set of sorts.
     * @incubating The current contract only supports sorts on logical metrics. This should be replaced with a more
     * generic option that allows implementors to sort on any data they desire. This may require deeper Fili changes
     * which a simple changing on the DARI are not sufficient to implement. In which case this incubating flag can be
     * removed once generic building hooks against a POJO DataApiRequest implementation are added.
     */
    @Incubating
    LinkedHashSet<OrderByColumn> generateSorts(
            LinkedHashMap<String, SortDirection> sortDirections,
            Set<LogicalMetric> logicalMetrics,
            MetricDictionary metricDictionary
    );

    /**
     * Validates that both the dateTime sort and the metric sorts were properly generated.
     *
     * @param sorts  The metrics sorts constructed by
     * {@link SortGenerator#generateSorts(LinkedHashMap, Set, MetricDictionary)}.
     * @param dateTimeSort  The dateTime sort, constructed by {@link SortGenerator#generateDateTimeSort(LinkedHashMap)}.
     * @param sortsRequest  The value of the sort parameter from the ApiRequest.
     * @param logicalMetrics  The set of logical metrics requested by the api request.
     * @param metricDictionary  The metric dictionary.
     */
    void validateSortColumns(
            LinkedHashSet<OrderByColumn> sorts,
            OrderByColumn dateTimeSort,
            String sortsRequest,
            LinkedHashSet<LogicalMetric> logicalMetrics,
            MetricDictionary metricDictionary
    );
}
