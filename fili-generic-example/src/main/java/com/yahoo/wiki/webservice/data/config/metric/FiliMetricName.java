package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.List;
import java.util.Locale;

/**
 * Created by kevin on 3/7/2017.
 */
public class FiliMetricName implements ApiMetricName {
    private String apiName;
    private List<TimeGrain> satisfyingGrains;

    public FiliMetricName(String lowerCaseName, List<TimeGrain> timeGrains) {
        this.apiName = EnumUtils.camelCase(lowerCaseName);
        satisfyingGrains = timeGrains;
    }

    @Override
    public String asName() {
        return getApiName();
    }

    @Override
    public String toString() {
        return apiName.toLowerCase(Locale.ENGLISH);
    }

    @Override
    public boolean isValidFor(TimeGrain grain) {
        return satisfyingGrains.stream().anyMatch(grain::satisfiedBy);
    }

    @Override
    public String getApiName() {
        return apiName;
    }
}
