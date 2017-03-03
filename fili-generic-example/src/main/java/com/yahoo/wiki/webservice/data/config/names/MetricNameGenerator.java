package com.yahoo.wiki.webservice.data.config.names;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Created by kevin on 2/28/2017.
 */
public class MetricNameGenerator {
    private static TimeGrain defaultTimeGrain;

    public static void setDefaultTimeGrain(TimeGrain t) {
        defaultTimeGrain = t;
    }

    public static FieldName getDruidMetric(String name, TimeGrain... timeGrains) {
        List<TimeGrain> timeGrainList = new ArrayList<>();
        timeGrainList.add(defaultTimeGrain);
        return new DruidMetricName(name, timeGrainList);
    }

    public static ApiMetricName getFiliMetricName(String name, TimeGrain... timeGrains) {
        ArrayList<TimeGrain> timeGrainList = new ArrayList<>();
        timeGrainList.add(defaultTimeGrain);
        return new FiliMetricName(name, timeGrainList);
    }

    private static class FiliMetricName implements ApiMetricName {
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

    private static class DruidMetricName implements FieldName {
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
}
