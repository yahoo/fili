// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.MetricField;

import java.util.Objects;

/**
 * A LogicalMetric is a set of its TemplateQueries, Mapper, and its name.
 */
public class LogicalMetric implements ApiMetricName {

    public static final String DEFAULT_CATEGORY = "General";

    private final TemplateDruidQuery query;
    private final ResultSetMapper calculation;
    private final String name;
    private final String longName;
    private final String category;
    private final String description;
    private final String type;

    /**
     * Build a fully specified Logical Metric.
     *
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param name  Name of the metric
     * @param longName  Long name of the metric
     * @param category  Category of the metric
     * @param description  Description of the metric
     *
     * @deprecated Properties, such as name, of LogicalMetric is stored in a unified object called
     * {@link com.yahoo.bard.webservice.data.metric.LogicalMetricInfo}. Use new constructor
     * {@link #LogicalMetric(TemplateDruidQuery, ResultSetMapper, LogicalMetricInfo)} instead.
     */
    @Deprecated
    public LogicalMetric(
            TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            String name,
            String longName,
            String category,
            String description
    ) {
        this(templateDruidQuery, calculation, new LogicalMetricInfo(name, longName, category, description));
    }

    /**
     * Build a slightly more specified Logical Metric.
     * <p>
     * Note: The description is set to the same as the name.
     *
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param name  Name of the metric
     * @param description  Description of the metric
     *
     * @deprecated Properties, such as name, of LogicalMetric is stored in a unified object called
     * {@link com.yahoo.bard.webservice.data.metric.LogicalMetricInfo}. Use new constructor
     * {@link #LogicalMetric(TemplateDruidQuery, ResultSetMapper, LogicalMetricInfo)} instead.
     */
    @Deprecated
    public LogicalMetric(
            TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            String name,
            String description
    ) {
        this(templateDruidQuery, calculation, new LogicalMetricInfo(name, name, DEFAULT_CATEGORY, description));
    }

    /**
     * Build a partly specified Logical Metric.
     * <p>
     * Note: The description is set to the same as the name.
     *
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param name  Name of the metric
     *
     * @deprecated Properties, such as name, of LogicalMetric is stored in a unified object called
     * {@link com.yahoo.bard.webservice.data.metric.LogicalMetricInfo}. Use new constructor
     * {@link #LogicalMetric(TemplateDruidQuery, ResultSetMapper, LogicalMetricInfo)} instead.
     */
    @Deprecated
    public LogicalMetric(TemplateDruidQuery templateDruidQuery, ResultSetMapper calculation, String name) {
        this(templateDruidQuery, calculation, new LogicalMetricInfo(name, name, DEFAULT_CATEGORY, name));
    }

    /**
     * Constructor. Builds a Logical Metric whose instance variables are provided by a LogicalMetricInfo object.
     *
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param logicalMetricInfo  Logical Metric info provider
     */
    public LogicalMetric(
            TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            LogicalMetricInfo logicalMetricInfo
    ) {
        this.calculation = calculation;
        this.name = logicalMetricInfo.getName();
        this.longName = logicalMetricInfo.getLongName();
        this.category = logicalMetricInfo.getCategory();
        this.description = logicalMetricInfo.getDescription();
        this.type = logicalMetricInfo.getType();
        this.query = templateDruidQuery;
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return description;
    }

    public ResultSetMapper getCalculation() {
        return this.calculation;
    }

    public TemplateDruidQuery getTemplateDruidQuery() {
        return query;
    }

    public MetricField getMetricField() {
        return getTemplateDruidQuery().getMetricField(getName());
    }

    public String getCategory() {
        return category;
    }

    public String getLongName() {
        return longName;
    }

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return "LogicalMetric{\n" +
                "name=" + name + ",\n" +
                "templateDruidQuery=" + query + ",\n" +
                "calculation=" + calculation + "\n" +
                "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        LogicalMetric that = (LogicalMetric) o;
        return
                Objects.equals(query, that.query) &&
                Objects.equals(calculation, that.calculation) &&
                Objects.equals(name, that.name) &&
                Objects.equals(longName, that.longName) &&
                Objects.equals(category, that.category) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, calculation, name, longName, category, description);
    }

    @Override
    public String asName() {
        return getName();
    }

    @Override
    public boolean isValidFor(TimeGrain grain) {
        return true;
    }

    @Override
    public String getApiName() {
        return getName();
    }
}
