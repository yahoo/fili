// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.MetricField;

import java.util.Objects;
import java.util.function.Predicate;

import javax.validation.constraints.NotNull;

/**
 * Logical Metrics model calculations performed in a request.
 */
public class LogicalMetricImpl implements LogicalMetric {

    public static final String DEFAULT_CATEGORY = "General";
    public static final Predicate<Granularity> ALWAYS_TRUE = grain -> true;

    private final TemplateDruidQuery query;
    private final ResultSetMapper calculation;

    protected LogicalMetricInfo logicalMetricInfo;

    protected final Predicate<Granularity> validGrains;

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
     * {@link com.yahoo.bard.webservice.data.metric.LogicalMetricInfo}. Use new constructor
     * {@link #LogicalMetricImpl(TemplateDruidQuery, ResultSetMapper, LogicalMetricInfo)} instead.
     */
    @Deprecated
    public LogicalMetricImpl(
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
     * {@link #LogicalMetricImpl(TemplateDruidQuery, ResultSetMapper, LogicalMetricInfo)} instead.
     */
    @Deprecated
    public LogicalMetricImpl(
            TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            String name,
            String description
    ) {
        this(templateDruidQuery, calculation, new LogicalMetricInfo(name, name, description));
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
     * {@link com.yahoo.bard.webservice.data.metric.LogicalMetricInfo}. Use new constructor
     * {@link #LogicalMetricImpl(TemplateDruidQuery, ResultSetMapper, LogicalMetricInfo)} instead.
     */
    public LogicalMetricImpl(TemplateDruidQuery templateDruidQuery, ResultSetMapper calculation, String name) {
        this(templateDruidQuery, calculation, new LogicalMetricInfo(name));
    }

    /**
     * Build a partly specified Logical Metric.
     * <p>
     * Note: The description is set to the same as the name.
     *
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param name  Name of the metric
     * @param validGrains a predicate describing valid granularities
     *
     * {@link com.yahoo.bard.webservice.data.metric.LogicalMetricInfo}. Use new constructor
     * {@link #LogicalMetricImpl(TemplateDruidQuery, ResultSetMapper, LogicalMetricInfo)} instead.
     */
    public LogicalMetricImpl(
            TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            String name,
            Predicate validGrains
    ) {
        this(templateDruidQuery, calculation, new LogicalMetricInfo(name), validGrains);
    }

    /**
     * Constructor. Builds a Logical Metric whose instance variables are provided by a LogicalMetricInfo object.
     *
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param logicalMetricInfo  Logical Metric info provider
     */
    public LogicalMetricImpl(
            TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            @NotNull LogicalMetricInfo logicalMetricInfo
    ) {
        this(templateDruidQuery, calculation, logicalMetricInfo, ALWAYS_TRUE);
    }

    /**
     * Constructor. Builds a Logical Metric whose instance variables are provided by a LogicalMetricInfo object.
     *
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param logicalMetricInfo  Logical Metric info provider
     * @param validGrains a predicate describing valid granularities
     */
    public LogicalMetricImpl(
            TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            @NotNull LogicalMetricInfo logicalMetricInfo,
            Predicate validGrains

    ) {
        this.calculation = calculation;
        this.logicalMetricInfo = logicalMetricInfo;
        this.query = templateDruidQuery;
        this.validGrains = validGrains;
    }
    @Override
    public String getName() {
        return logicalMetricInfo.getName();
    }

    @Override
    public String getDescription() {
        return logicalMetricInfo.getDescription();
    }

    @Override
    public ResultSetMapper getCalculation() {
        return this.calculation;
    }

    @Override
    public LogicalMetricInfo getLogicalMetricInfo() {
        return logicalMetricInfo;
    }

    @Override
    public TemplateDruidQuery getTemplateDruidQuery() {
        return query;
    }

    @Override
    public MetricField getMetricField() {
        return getTemplateDruidQuery().getMetricField(getName());
    }

    @Override
    public String getCategory() {
        return logicalMetricInfo.getCategory();
    }

    @Override
    public String getLongName() {
        return logicalMetricInfo.getLongName();
    }

    @Override
    public String getType() {
        return logicalMetricInfo.getType();
    }

    /**
     * Make a copy of this metric with a modified granularity predicate.
     *
     * @param granularityPredicate  A predicate describing the the valid time grains.
     *
     * @return  A modified copy of the metric.
     */
    public LogicalMetric withValidFor(Predicate<Granularity> granularityPredicate) {
        return new LogicalMetricImpl(query, calculation, logicalMetricInfo, granularityPredicate);
    }

    @Override
    public String toString() {
        return "LogicalMetric{\n" +
                "name=" + logicalMetricInfo.getName() + ",\n" +
                "templateDruidQuery=" + query + ",\n" +
                "calculation=" + calculation + "\n" +
                "}";
    }

    @Override
    public boolean isValidFor(Granularity granularity) {
        return validGrains.test(granularity);
    }


        @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        LogicalMetricImpl that = (LogicalMetricImpl) o;
        return
                Objects.equals(query, that.query) &&
                Objects.equals(calculation, that.calculation) &&
                Objects.equals(logicalMetricInfo, that.logicalMetricInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, calculation, logicalMetricInfo);
    }
}
