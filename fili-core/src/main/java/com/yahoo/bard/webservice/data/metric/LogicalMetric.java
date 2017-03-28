// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * A LogicalMetric is a set of its TemplateQueries, Mapper, and its name.
 */
public class LogicalMetric {

    public static final String DEFAULT_CATEGORY = "General";

    private final TemplateDruidQuery query;
    private final ResultSetMapper calculation;
    private final String name;
    private final String longName;
    private final String category;
    private final String description;

    private final Predicate<Granularity> isValidFor;

    /**
     * Build a fully specified Logical Metric.
     *
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param name  Name of the metric
     * @param longName Long name of the metric
     * @param category  Category of the metric
     * @param description  Description of the metric
     * @param isValidFor  A predicate defining which granularities this metric is valid for
     */
    public LogicalMetric(
            TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            String name,
            String longName,
            String category,
            String description,
            Predicate<Granularity> isValidFor
    ) {
        this.calculation = calculation;
        this.name = name;
        this.longName = longName;
        this.category = category;
        this.description = description;
        this.query = templateDruidQuery;
        this.isValidFor = isValidFor;
    }

    /**
     * Build a fully specified Logical Metric.
     *
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param name  Name of the metric
     * @param longName Long name of the metric
     * @param category  Category of the metric
     * @param description  Description of the metric
     *
     * @deprecated use explicit Predicates for validity checks
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
        this(templateDruidQuery, calculation, name, longName, category, description, (Predicate<Granularity>) null);
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
     * @param isValidFor  A predicate defining which granularities this metric is valid for
     *
     */
    public LogicalMetric(
            TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            String name,
            String description,
            Predicate<Granularity> isValidFor
    ) {
        this(templateDruidQuery, calculation, name, name, DEFAULT_CATEGORY, description, isValidFor);
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
     * @deprecated use explicit Predicates for validity checks
     */
    @Deprecated
    public LogicalMetric(
            TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            String name,
            String description
    ) {
        this(templateDruidQuery, calculation, name, name, DEFAULT_CATEGORY, description, (Predicate<Granularity>) null);
    }

    /**
     * Build a partly specified Logical Metric.
     * <p>
     * Note: The description is set to the same as the name.
     *
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param name  Name of the metric
     * @param isValidFor  A predicate defining which granularities this metric is valid for
     */
    public LogicalMetric(
            TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            String name,
            Predicate<Granularity> isValidFor
    ) {
        this(templateDruidQuery, calculation, name, name, DEFAULT_CATEGORY, name, isValidFor);
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
     * @deprecated use explicit Predicates for validity checks
     */
    @Deprecated
    public LogicalMetric(TemplateDruidQuery templateDruidQuery, ResultSetMapper calculation, String name) {
        this(templateDruidQuery, calculation, name, name, DEFAULT_CATEGORY, name, (Predicate<Granularity>) null);
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

    @Override
    public String toString() {
        return "LogicalMetric{\n" +
                "name=" + name + ",\n" +
                "templateDruidQuery=" + query + ",\n" +
                "calculation=" + calculation + "\n" +
                "}";
    }

    public String getCategory() {
        return category;
    }

    public String getLongName() {
        return longName;
    }

    public Predicate<Granularity> getIsValidFor() {
        return isValidFor;
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
}
