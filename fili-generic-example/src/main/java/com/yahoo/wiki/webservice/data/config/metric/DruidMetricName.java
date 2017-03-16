package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.names.FieldName;

/**
 * Created by kevin on 3/7/2017.
 */
public class DruidMetricName implements FieldName {
    private String name;

    public DruidMetricName(String name) {
        this.name = name;
    }

    @Override
    public String asName() {
        return toString();
    }

    @Override
    public String toString() {
        return name;
    }
}
