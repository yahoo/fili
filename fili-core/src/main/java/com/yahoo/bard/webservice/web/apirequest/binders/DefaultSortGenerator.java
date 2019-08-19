package com.yahoo.bard.webservice.web.apirequest.binders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DATE_TIME_SORT_VALUE_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_METRICS_NOT_IN_QUERY_FORMAT;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_METRICS_NOT_SORTABLE_FORMAT;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_METRICS_UNDEFINED;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.util.Incubating;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Incubating
public class DefaultSortGenerator implements SortGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(DataApiRequestImpl.class);

    @Override
    public OrderByColumn generateDateTimeSort(LinkedHashMap<String, SortDirection> sortDirections) {
        if (sortDirections != null && sortDirections.containsKey(DATE_TIME_STRING)) {
            if (!isDateTimeFirstSortField(sortDirections)) {
                LOG.debug(DATE_TIME_SORT_VALUE_INVALID.logFormat());
                throw new BadApiRequestException(DATE_TIME_SORT_VALUE_INVALID.format());
            }
            return new OrderByColumn(DATE_TIME_STRING, sortDirections.get(DATE_TIME_STRING));
        }
        return null;
    }

    @Override
    public LinkedHashSet<OrderByColumn> generateSorts(
            LinkedHashMap<String, SortDirection> sortDirections,
            Set<LogicalMetric> logicalMetrics,
            MetricDictionary metricDictionary
    ) {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingSortColumns")) {
            String sortMetricName;
            LinkedHashSet<OrderByColumn> metricSortColumns = new LinkedHashSet<>();

            if (sortDirections == null) {
                return metricSortColumns;
            }
            List<String> unknownMetrics = new ArrayList<>();
            List<String> unmatchedMetrics = new ArrayList<>();
            List<String> unsortableMetrics = new ArrayList<>();

            for (Map.Entry<String, SortDirection> entry : sortDirections.entrySet())  {
                sortMetricName = entry.getKey();

                LogicalMetric logicalMetric = metricDictionary.get(sortMetricName);

                // If metric dictionary returns a null, it means the requested sort metric is not found.
                if (logicalMetric == null) {
                    unknownMetrics.add(sortMetricName);
                    continue;
                }
                if (!logicalMetrics.contains(logicalMetric)) {
                    unmatchedMetrics.add(sortMetricName);
                    continue;
                }
                if (logicalMetric.getTemplateDruidQuery() == null) {
                    unsortableMetrics.add(sortMetricName);
                    continue;
                }
                metricSortColumns.add(new OrderByColumn(logicalMetric, entry.getValue()));
            }
            if (!unknownMetrics.isEmpty()) {
                LOG.debug(SORT_METRICS_UNDEFINED.logFormat(unknownMetrics.toString()));
                throw new BadApiRequestException(SORT_METRICS_UNDEFINED.format(unknownMetrics.toString()));
            }
            if (!unmatchedMetrics.isEmpty()) {
                LOG.debug(SORT_METRICS_NOT_IN_QUERY_FORMAT.logFormat(unmatchedMetrics.toString()));
                throw new BadApiRequestException(SORT_METRICS_NOT_IN_QUERY_FORMAT.format(unmatchedMetrics.toString()));
            }
            if (!unsortableMetrics.isEmpty()) {
                LOG.debug(SORT_METRICS_NOT_SORTABLE_FORMAT.logFormat(unsortableMetrics.toString()));
                throw new BadApiRequestException(SORT_METRICS_NOT_SORTABLE_FORMAT.format(unsortableMetrics.toString()));
            }

            return metricSortColumns;
        }
    }

    @Override
    public void validateSortColumns(
            LinkedHashSet<OrderByColumn> sorts,
            OrderByColumn dateTimeSort,
            String sortsRequest,
            LinkedHashSet<LogicalMetric> logicalMetrics,
            MetricDictionary metricDictionary
    ) {
        // no default validation
    }

    /**
     * To check whether dateTime column request is first one in the sort list or not.
     *
     * @param sortColumns  LinkedHashMap of columns and its direction. Using LinkedHashMap to preserve the order
     *
     * @return True if dateTime column is first one in the sort list. False otherwise
     */
    protected Boolean isDateTimeFirstSortField(LinkedHashMap<String, SortDirection> sortColumns) {
        if (sortColumns != null) {
            List<String> columns = new ArrayList<>(sortColumns.keySet());
            return columns.get(0).equals(DATE_TIME_STRING);
        } else {
            return false;
        }
    }
}
