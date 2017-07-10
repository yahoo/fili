package com.yahoo.bard.webservice.sql.aggregation;

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import java.util.Optional;

/**
 * Created by hinterlong on 7/10/17.
 */
public interface DruidSqlTypeConverter {

    /**
     * Finds the corresponding {@link SqlAggregationType} from a
     * druid aggregation type.
     *
     * @param type  The druid aggregation type, i.e. "longSum".
     *
     * @return the supported sql aggregation type.
     */
    Optional<SqlAggregationType> fromDruidType(String type);

    default Optional<SqlAggregationType> fromDruidType(Aggregation aggregation) {
        return fromDruidType(aggregation.getType());
    }
}
