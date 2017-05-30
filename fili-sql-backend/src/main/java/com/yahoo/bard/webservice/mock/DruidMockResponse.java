package com.yahoo.bard.webservice.mock;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hinterlong on 5/31/17.
 */
@JsonSerialize(using = CustomDruidSerializer.class)
public class DruidMockResponse {
    public List<TimeStampResult> results = new ArrayList<>();

    public static class TimeStampResult {
        @JsonIgnore
        public DateTime timestamp;
        @JsonIgnore
        public Result result = new Result();

        @JsonProperty
        public String getTimestamp() {
            return timestamp.toString();
        }

        @JsonProperty
        public Map<String, Object> getResult() {
            return result.resultsMap;
        }

        public static class Result {
            public Map<String, Object> resultsMap = new HashMap<>();
        }

    }
}
