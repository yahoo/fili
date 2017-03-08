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
    private List<TimeGrain> satisfyingGrains;

    public DruidMetricName(String lowerCaseName, List<TimeGrain> timeGrains) {
        this.lowerCaseName = lowerCaseName.toLowerCase(Locale.ENGLISH);
        satisfyingGrains = timeGrains;
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
