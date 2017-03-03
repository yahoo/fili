package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.List;

/**
 * Created by kevin on 3/3/2017.
 */
public interface DruidConfig {
    public List<String> getMetrics();
    public List<String> getDimensions();
    public List<TimeGrain> getValidTimeGrains();
}
