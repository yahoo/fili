package com.yahoo.bard.webservice.data.config;

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;

public interface ConfigurationLoader {
    void load();

    DimensionDictionary getDimensionDictionary();

    MetricDictionary getMetricDictionary();

    LogicalTableDictionary getLogicalTableDictionary();

    PhysicalTableDictionary getPhysicalTableDictionary();

    ResourceDictionaries getDictionaries();
}
