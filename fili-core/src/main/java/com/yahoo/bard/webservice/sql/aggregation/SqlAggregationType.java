package com.yahoo.bard.webservice.sql.aggregation;

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;

/**
 * Created by hinterlong on 7/10/17.
 */
public interface SqlAggregationType {
    /**
     * Builds an aggregate call using the {@link SqlAggFunction} corresponding
     * to the aggregation type.
     *
     * @param builder  The RelBuilder used with calcite to build queries.
     * @param aggregation  The druid aggregation.
     *
     * @return the AggCal built from the aggregation type.
     */
    RelBuilder.AggCall getAggregation(RelBuilder builder, Aggregation aggregation);
}
