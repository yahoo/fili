// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.data.metric.mappers.RenamableResultSetMapper;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.druid.model.MetricField;

import java.util.Objects;

import javax.validation.constraints.NotNull;

/**
 * A LogicalMetric is a set of its TemplateQueries, Mapper, and its name.
 * <p>
 * All classes that extend LogicalMetricImpl MUST override the
 * {@link LogicalMetric#withLogicalMetricInfo(LogicalMetricInfo)} to return the extended subclass. Otherwise, any code
 * that relies on the behavior of that method will cause the subclass to be lost.
 */
public class LogicalMetricImpl implements LogicalMetric {

    private final TemplateDruidQuery query;
    private final ResultSetMapper calculation;
    protected LogicalMetricInfo logicalMetricInfo;

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
    public LogicalMetricImpl(
            @NotNull TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            @NotNull String name,
            String longName,
            String category,
            String description
    ) {
        this(new LogicalMetricInfo(name, longName, category, description), templateDruidQuery, calculation);
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
            @NotNull TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            @NotNull String name,
            String description
    ) {
        this(new LogicalMetricInfo(name, name, DEFAULT_CATEGORY, description), templateDruidQuery, calculation);
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
     * {@link #LogicalMetricImpl(TemplateDruidQuery, ResultSetMapper, LogicalMetricInfo)} instead.
     */
    @Deprecated
    public LogicalMetricImpl(
            @NotNull TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            @NotNull String name
    ) {
        this(new LogicalMetricInfo(name, name, DEFAULT_CATEGORY, name), templateDruidQuery, calculation);
    }

    /**
     * Constructor. Builds a Logical Metric whose instance variables are provided by a LogicalMetricInfo object.
     *
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param logicalMetricInfo  Logical Metric info provider
     *
     * @deprecated use {@link #LogicalMetricImpl(LogicalMetricInfo, TemplateDruidQuery, ResultSetMapper)}
     */
    @Deprecated
    public LogicalMetricImpl(
            @NotNull TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            @NotNull LogicalMetricInfo logicalMetricInfo
    ) {
        this.calculation = calculation;
        this.logicalMetricInfo = logicalMetricInfo;
        this.query = templateDruidQuery;
    }

    /**
     * Constructor. Builds a Logical Metric whose instance variables are provided by a LogicalMetricInfo object.
     *
     * @param logicalMetricInfo  Logical Metric info provider
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     */
    public LogicalMetricImpl(
            @NotNull LogicalMetricInfo logicalMetricInfo,
            @NotNull TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation
    ) {
        this.logicalMetricInfo = logicalMetricInfo;
        this.calculation = calculation;
        this.query = templateDruidQuery;
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

    @Override
    public LogicalMetricInfo getLogicalMetricInfo() {
        return logicalMetricInfo;
    }

    /**
     * All subclasses of {@code LogicalMetricImpl} MUST override this method and return an instance of the subclassed
     * type. Inheriting this implementation on subclasses will cause the subclass typing to be lost!
     *
     * @inheritDocs
     */
    @Override
    public LogicalMetric withLogicalMetricInfo(LogicalMetricInfo info) {
        return new LogicalMetricImpl(
                info,
                renameTemplateDruidQuery(info.getName()),
                renameResultSetMapper(info.getName())
        );
    }

    /**
     * Convenience method for renaming the MetricField that this LogicalMetric represents on the TemplateDruidQuery for
     * this LogicalMetric.
     *
     * @param newName  The name for the output MetricField to be renamed to
     * @return the renamed TemplateDruidQuery
     */
    protected TemplateDruidQuery renameTemplateDruidQuery(String newName) {
        return renameTemplateDruidQuery(getTemplateDruidQuery(), getLogicalMetricInfo().getName(), newName);
    }

    /**
     * Renames the output name of the MetricField on the outermost level of the provided tdq. The input
     * {@link TemplateDruidQuery} MAY be null. If so, this method simply returns null. If {@code query} is NOT null, a
     * {@link MetricField} with name {@code oldName} MUST be present on the query.
     *
     * @param query  Tdq to perform the rename over
     * @param oldName  The name of the MetricField to rename
     * @param newName  The new output name for the MetricField
     * @return the renamed TemplateDruidQuery
     * @throws IllegalArgumentException if {@code query} is not null AND does not contain a MetricField with output name
     *                                  {@code oldName}
     */
    protected TemplateDruidQuery renameTemplateDruidQuery(
            TemplateDruidQuery query,
            String oldName,
            String newName
    ) {
        if (query == null) {
            return query;
        }
        if (!query.containsMetricField(oldName)) {
            throw new IllegalArgumentException(
                    String.format(
                            TemplateDruidQuery.NO_METRIC_TO_RENAME_FOUND_ERROR_MESSAGE,
                            oldName
                    )
            );
        }
        return query.renameMetricField(oldName, newName);
    }

    /**
     * Convenience method for renaming result set mapper. Calls the rename method with this metric's result set mapper.
     *
     * @param newName  The column name to point the result set mapper at, if applicable
     * @return either this metric's mapper repointed at the new column name if applicable.
     */
    protected ResultSetMapper renameResultSetMapper(String newName) {
        return renameResultSetMapper(getCalculation(), newName);
    }

    /**
     * If the mapper is renamable, points the mapper at the new nae. Otherwise, just returns the input mapper.
     *
     * @param mapper  The mapper to rename, if applicable
     * @param newName  The new column name for the mapper to use, if applicable
     * @return the input mapper if {@code mapper} is not renamable, or the renamed mapper if it is
     */
    protected ResultSetMapper renameResultSetMapper(ResultSetMapper mapper, String newName) {
        if (mapper instanceof RenamableResultSetMapper) {
            return ((RenamableResultSetMapper) mapper).withColumnName(newName);
        }
        return mapper;
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
