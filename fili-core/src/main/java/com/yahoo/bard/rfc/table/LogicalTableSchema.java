// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.rfc.table;

import com.yahoo.bard.rfc.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.metric.LogicalMetricColumn;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.TableGroup;

import java.util.LinkedHashSet;
import java.util.stream.Collectors;

public class LogicalTableSchema extends LinkedHashSet<Column> implements Schema  {

    public LogicalTableSchema(TableGroup tableGroup, MetricDictionary metricDictionary) {
        addAll(
                tableGroup.getDimensions().stream()
                        .map(DimensionColumn::new)
                        .collect(Collectors.toSet())
        );
        addAll(
                tableGroup.getApiMetricNames().stream()
                        .map(ApiMetricName::getApiName)
                        .map(name -> new LogicalMetricColumn(name, metricDictionary.get(name)))
                        .collect(Collectors.toSet())
        );
    }
}
