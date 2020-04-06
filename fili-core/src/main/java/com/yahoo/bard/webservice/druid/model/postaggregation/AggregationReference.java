package com.yahoo.bard.webservice.druid.model.postaggregation;

import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * A PostAggregation that references and Aggregation. While this cannot be validated using Java's type system, the
 * behavior of classes that interact with contract is ONLY defined on PostAggregations.
 */
public interface AggregationReference<T extends PostAggregation> extends MetricField {

    /**
     * Gets the aggregations this PostAggregation depends on
     * @return
     */
    @JsonIgnore
    List<Aggregation> getAggregations();

    /**
     * Return a copy of this with the dependent aggregations replaced by a COPY of the input aggregations list.
     * @param aggregations
     * @return
     */
    T withAggregations(List<Aggregation> aggregations);
}
