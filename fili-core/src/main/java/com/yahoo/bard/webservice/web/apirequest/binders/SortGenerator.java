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

@Incubating
public interface SortGenerator {

    String DATE_TIME_STRING = "dateTime";

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

    OrderByColumn generateDateTimeSort(LinkedHashMap<String, SortDirection> sortDirections);

    LinkedHashSet<OrderByColumn> generateSorts(
            LinkedHashMap<String, SortDirection> sortDirections,
            Set<LogicalMetric> logicalMetrics,
            MetricDictionary metricDictionary
    );

    void validateSortColumns(
            LinkedHashSet<OrderByColumn> sorts,
            OrderByColumn dateTimeSort,
            String sortsRequest,
            LinkedHashSet<LogicalMetric> logicalMetrics,
            MetricDictionary metricDictionary
    );

}
