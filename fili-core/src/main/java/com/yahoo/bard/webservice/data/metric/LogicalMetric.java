// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;

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

    /**
     * Build a fully specified Logical Metric.
     *
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param name  Name of the metric
     * @param longName Long name of the metric
     * @param category  Category of the metric
     * @param description  Description of the metric
     */
    public LogicalMetric(
            TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            String name,
            String longName,
            String category,
            String description
    ) {
        this.calculation = calculation;
        this.name = name;
        this.longName = longName;
        this.category = category;
        this.description = description;
        this.query = templateDruidQuery;
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
     */
    public LogicalMetric(
            TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            String name,
            String description
    ) {
        this(templateDruidQuery, calculation, name, name, DEFAULT_CATEGORY, description);
    }

    /**
     * Build a partly specified Logical Metric.
     * <p>
     * Note: The description is set to the same as the name.
     *
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param name  Name of the metric
     */
    public LogicalMetric(TemplateDruidQuery templateDruidQuery, ResultSetMapper calculation, String name) {
        this(templateDruidQuery, calculation, name, name, DEFAULT_CATEGORY, name);
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

    // CHECKSTYLE:OFF
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogicalMetric that = (LogicalMetric) o;

        if (query != null ? !query.equals(that.query) : that.query != null) return false;
        if (calculation != null ? !calculation.equals(that.calculation) : that.calculation != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (longName != null ? !longName.equals(that.longName) : that.longName != null) return false;
        if (category != null ? !category.equals(that.category) : that.category != null) return false;
        return !(description != null ? !description.equals(that.description) : that.description != null);
    }

    @Override
    public int hashCode() {
        int result = query != null ? query.hashCode() : 0;
        result = 31 * result + (calculation != null ? calculation.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (longName != null ? longName.hashCode() : 0);
        result = 31 * result + (category != null ? category.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        return result;
    }
    // CHECKSTYLE:ON
}
