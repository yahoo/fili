package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;

import java.util.List;

public class RegenerativeMetric extends LogicalMetric {

    final MetricMaker metricMaker;
    final List<String> dependencies;
    final List<String> acceptingParameters;

    public RegenerativeMetric(
            final TemplateDruidQuery templateDruidQuery,
            final ResultSetMapper calculation,
            final String name,
            final String longName,
            final String category,
            final String description,
            MetricMaker metricMaker,
            List<String> dependencies,
            List<String> acceptingParameters
    ) {
        super(templateDruidQuery, calculation, name, longName, category, description);
        this.metricMaker = metricMaker;
        this.dependencies = dependencies;
        this.acceptingParameters = acceptingParameters;
    }
}
