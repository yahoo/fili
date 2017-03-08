package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.List;

/**
 * Created by kevin on 3/7/2017.
 */
public class FiliMetricName implements ApiMetricName {
    private String apiName;
    private List<TimeGrain> satisfyingGrains;

    public FiliMetricName(String name, List<TimeGrain> timeGrains) {
        this.apiName = name;
        satisfyingGrains = timeGrains;
    }

    @Override
    public String asName() {
        return getApiName();
    }

    @Override
    public String toString() {
        return apiName;
    }

    @Override
    public boolean isValidFor(TimeGrain grain) {
        return satisfyingGrains.stream().anyMatch(grain::satisfiedBy);
    }

    @Override
    public String getApiName() {
        return toString();
    }
}
