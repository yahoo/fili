package com.yahoo.bard.webservice.sql.aggregation;

import java.util.Locale;
import java.util.Optional;

/**
 * Created by hinterlong on 7/10/17.
 */
public class DefaultDruidSqlTypeConverter implements DruidSqlTypeConverter {

    public DefaultDruidSqlTypeConverter() {

    }

    @Override
    public Optional<SqlAggregationType> fromDruidType(String type) {
        for (DefaultSqlAggregationType aggregationType : DefaultSqlAggregationType.values()) {
            if (type.toLowerCase(Locale.ENGLISH).contains(aggregationType.type)) {
                return Optional.of(aggregationType);
            }
        }
        return Optional.empty();
    }
}
