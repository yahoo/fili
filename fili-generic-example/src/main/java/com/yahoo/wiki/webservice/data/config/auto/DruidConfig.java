// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.List;

/**
 * Created by kevin on 3/3/2017.
 */
public interface DruidConfig {
    public String getName();

    public TableName getTableName();

    public List<String> getMetrics();

    public List<String> getDimensions();

    public List<TimeGrain> getValidTimeGrains();
}
