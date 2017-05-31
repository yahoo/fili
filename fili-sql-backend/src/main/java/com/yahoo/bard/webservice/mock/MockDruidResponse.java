package com.yahoo.bard.webservice.mock;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hinterlong on 5/31/17.
 */
@JsonSerialize(using = MockDruidResponseSerializer.class)
public class MockDruidResponse {
    public final List<TimeStampResult> results = new ArrayList<>();

    public static class TimeStampResult {
        @JsonIgnore
        public final DateTime timestamp;
        @JsonIgnore
        public final Result result = new Result();

        public TimeStampResult(DateTime timestamp) {
            this.timestamp = timestamp;
        }

        public void add(String key, Object value) {
            result.resultsMap.put(key, value);
        }

        @JsonProperty
        public String getTimestamp() {
            return timestamp.toDateTime(DateTimeZone.UTC).toString();
        }

        @JsonProperty
        public Map<String, Object> getResult() {
            return result.resultsMap;
        }

        public static class Result {
            @JsonProperty
            final Map<String, Object> resultsMap = new HashMap<>();
        }

    }
}
