package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.List;
import java.util.Locale;

/**
 * Created by kevin on 3/7/2017.
 */
public class DruidMetricName implements FieldName {
    private String lowerCaseName;

    public DruidMetricName(String lowerCaseName) {
        this.lowerCaseName = lowerCaseName.toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String asName() {
        return toString();
    }

    @Override
    public String toString() {
        return lowerCaseName;
    }
}
