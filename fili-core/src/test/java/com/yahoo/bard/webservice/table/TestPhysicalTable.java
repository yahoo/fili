// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ConfigPhysicalTable implementation. Used for easily building physical tables to test against.
 */
public class TestPhysicalTable implements ConfigPhysicalTable {

    /**
     * Builder class for making constructing a custom test physical table easily.
     */
    public static class Builder {
        private final DimensionDictionary dimensionDictionary;

        private TableName name;
        private Set<DataSourceName> dsNames;
        private ZonedTimeGrain grain = new ZonedTimeGrain(DefaultTimeGrain.DAY, DateTimeZone.UTC);
        private Map<String, String> logicalToPhysicalColumns = new HashMap<>();
        private Set<String> dimensionNames = new HashSet<>();
        private Set<String> metricNames = new HashSet<>();
        private Map<String, SimplifiedIntervalList> availabilityMap = new HashMap<>();

        /**
         * Basic constructor. Name is required, all other parameters are defaulted.
         *
         * @param name  Name of the physical table
         * @param dimensionDictionary  The dimension dictionary
         */
        public Builder(TableName name, DimensionDictionary dimensionDictionary) {
            this.name = name;
            this.dsNames = Collections.singleton(name::asName);
            this.dimensionDictionary = dimensionDictionary;
        }

        /**
         * Copy constructor. Copies the given physical table in this builder, which you can then build on.
         *  @param basePhysicalTable  The base physical table to build off of
         * @param name  The name of the new physical table to be built
         * @param dimensionDictionary  The dimension dictionary
         */
        public Builder(PhysicalTable basePhysicalTable, TableName name, DimensionDictionary dimensionDictionary) {
            this.name = name;
            this.dsNames = basePhysicalTable.getDataSourceNames();
            this.grain = basePhysicalTable.getSchema().getTimeGrain();
            this.dimensionNames = basePhysicalTable.getSchema()
                    .getColumns(DimensionColumn.class)
                    .stream()
                    .map(Column::getName)
                    .collect(Collectors.toCollection(LinkedHashSet::new));

            this.metricNames = basePhysicalTable.getSchema()
                    .getColumns(MetricColumn.class)
                    .stream()
                    .map(Column::getName)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            this.availabilityMap = basePhysicalTable.getAllAvailableIntervals().entrySet().stream()
                    .map(entry -> new AbstractMap.SimpleImmutableEntry<>(
                            entry.getKey().getName(),
                            entry.getValue()
                    ))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                            )
                    );

            this.logicalToPhysicalColumns = basePhysicalTable.getSchema().getColumns().stream()
                    .map(cn ->
                            new AbstractMap.SimpleImmutableEntry<>(
                                    cn.getName(),
                                    basePhysicalTable.getSchema().getPhysicalColumnName(cn.getName())
                            )
                    )
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                            )
                    );
            this.dimensionDictionary = dimensionDictionary;
        }

        /**
         * Sets the datasource names.
         *
         * @param dsNames  The datasource names
         * @return the builder
         */
        public TestPhysicalTable.Builder setDataSourceNames(Set<DataSourceName> dsNames) {
            this.dsNames = dsNames;
            return this;
        }

        /**
         * Sets the time grain.
         *
         * @param grain  The grain
         * @return the builder
         */
        public TestPhysicalTable.Builder setGrain(ZonedTimeGrain grain) {
            this.grain = grain;
            return this;
        }

        /**
         * Sets the names of the dimensions on this table. These names will be resolved from the
         * {@link DimensionDictionary} which was provided in the constructor. Ensure all names are resolvable to
         * {@link Dimension} objects in the dictionary.
         *
         * @param names The dimension names
         * @return the builder
         */
        public TestPhysicalTable.Builder setDimensionNames(Set<String> names) {
            this.dimensionNames = new HashSet<>(names);
            return this;
        }

        /**
         * Adds a name to the existing set of dimensions this table will support. This name will be resolved from the
         * {@link DimensionDictionary} provided in the constructor. Ensure all names are resolvable to {@link Dimension}
         * objects in the dictionary.
         * @param name  The dimension name
         * @return the builder
         */
        public TestPhysicalTable.Builder addDimensionName(String name) {
            dimensionNames.add(name);
            return this;
        }

        /**
         * Sets the names of the metrics on this table. These names will be resolved from the {@link MetricDictionary}
         * AT QUERY TIME IF REQUESTED. The metric does not have to be in the dictionary for the table construction to
         * work.
         *
         * @param names  The metric names
         * @return the builder
         */
        public TestPhysicalTable.Builder setMetricNames(Set<String> names) {
            this.metricNames = new HashSet<>(names);
            return this;
        }

        /**
         * Adds a single metric name to the set of metrics supported on this table. These names will be resolved from
         * the {@link MetricDictionary} AT QUERY TIME IF REQUESTED. The metric does not have to be in the dictionary for
         * the table construction to work.
         *
         * @param name The metric name
         * @return the builder
         */
        public TestPhysicalTable.Builder addMetricName(String name) {
            metricNames.add(name);
            return this;
        }

        /**
         * Sets the mapping of logical columns this table supports to their backing physical columns. Any column that
         * does not have a mapping is mapped to itself when the {@code build} method is called.
         *
         * @param logicalToPhysicalColumns  The mapping of logical to physical columns.
         * @return the builder
         */
        public TestPhysicalTable.Builder setLogicalToPhysicalColumns(Map<String, String> logicalToPhysicalColumns) {
            this.logicalToPhysicalColumns = new HashMap<>(logicalToPhysicalColumns);
            return this;
        }

        /**
         * Add a mapping from a logical column to a physical column. Any column that does not have a mapping is mapped
         * to itself when the {@code build} method is called.
         *
         * @param logicalColumn  The logical column name
         * @param physicalColumn  The physical column name
         * @return the builder
         */
        public TestPhysicalTable.Builder addLogicalToPhysicalColumnMapping(
                String logicalColumn,
                String physicalColumn
        ) {
            logicalToPhysicalColumns.put(logicalColumn, physicalColumn);
            return this;
        }

        /**
         * Sets the mapping of column name to available intervals. Any column not set here will be set to always
         * available when the {@code build} method is called.
         *
         * @param mapping The mapping
         * @return the builder
         */
        public TestPhysicalTable.Builder setAvailabilityMap(Map<String, SimplifiedIntervalList> mapping) {
            this.availabilityMap = new HashMap<>(mapping);
            return this;
        }

        /**
         * Adds a mapping from a column name to its available intervals. Any column that does not have an availability
         * mapping will be marked as always available when the {@code build} method is called.
         * @param column  The column name
         * @param availability  That column's availability
         * @return the builder
         */
        public TestPhysicalTable.Builder addAvailabilityMapping(String column, SimplifiedIntervalList availability) {
            availabilityMap.put(column, availability);
            return this;
        }

        /**
         * Build method. Constructs the physical table based on the defaults and values set through the setter.
         * Dimension column names are resolved to dimensions during the building.
         *
         * @return the physical table.
         */
        public ConfigPhysicalTable build() {
            Stream.concat(
                    metricNames.stream(),
                    dimensionNames.stream()
            )
                    .filter(name -> !logicalToPhysicalColumns.containsKey(name))
                    .forEach(name -> logicalToPhysicalColumns.put(name, name));

            Stream.concat(
                    metricNames.stream(),
                    dimensionNames.stream()
            )
                    .filter(name -> !availabilityMap.containsKey(name))
                    .forEach(name ->
                            availabilityMap.put(name, new SimplifiedIntervalList(
                                    Collections.singleton(new Interval(
                                            Availability.DISTANT_PAST,
                                            Availability.FAR_FUTURE
                                    )))));

            return new TestPhysicalTable(
                    this.name,
                    this.grain,
                    this.dsNames,
                    this.dimensionNames
                            .stream()
                            .map(dimensionDictionary::findByApiName)
                            .map(DimensionColumn::new)
                            .collect(Collectors.toCollection(LinkedHashSet::new)),
                    this.metricNames
                            .stream()
                            .map(MetricColumn::new)
                            .collect(Collectors.toCollection(LinkedHashSet::new)),
                    this.logicalToPhysicalColumns,
                    this.availabilityMap
            );
        }
    }


    private final TableName name;
    private final Set<DataSourceName> dsNames;
    private final ZonedTimeGrain grain;
    private final Set<Column> dimensions;
    private final Set<Column> metrics;
    private final Set<Column> allColumns;
    private final Map<String, String> logicalToPhysicalColumns;
    private final Map<Column, SimplifiedIntervalList> availabilities;

    private final PhysicalTableSchema schema;
    private Availability availability;

    /**
     * Constructor. Private, use builder to create an instance.
     *
     * @param name  Name of the table
     * @param grain  Grain of the table
     * @param dsNames  Datasource names
     * @param dimensions  Dimensions on this table
     * @param metrics  Metrics on this table
     * @param logicalToPhysicalColumns  Mapping of logical column names to their backing physical column names.
     * @param availability  Mapping of column name to availability.
     */
    private TestPhysicalTable(
            TableName name,
            ZonedTimeGrain grain,
            Set<DataSourceName> dsNames,
            Set<Column> dimensions,
            Set<Column> metrics,
            Map<String, String> logicalToPhysicalColumns,
            Map<String, SimplifiedIntervalList> availability
    ) {
        this.name = name;
        this.grain = grain;
        this.dsNames = Collections.unmodifiableSet(new HashSet<>(dsNames));
        this.dimensions = Collections.unmodifiableSet(new HashSet<>(dimensions));
        this.metrics = Collections.unmodifiableSet(new HashSet<>(metrics));
        this.allColumns = Stream.concat(this.metrics.stream(), this.dimensions.stream())
                .collect(Collectors.collectingAndThen(
                        Collectors.toCollection(LinkedHashSet::new),
                        Collections::unmodifiableSet
                ));
        this.logicalToPhysicalColumns = Collections.unmodifiableMap(new HashMap<>(logicalToPhysicalColumns));

        this.availabilities = availability.entrySet().stream()
                .map(entry ->
                        new AbstractMap.SimpleImmutableEntry<>(
                                findColumnForName(entry.getKey(), this.allColumns),
                                entry.getValue()
                        )
                ).collect(
                        Collectors.collectingAndThen(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue
                                ),
                                Collections::unmodifiableMap
                        )
                );

        this.schema = new PhysicalTableSchema(
                this.grain,
                this.allColumns,
                this.logicalToPhysicalColumns
        );

        this.availability = new TestPhysicalTableAvailability();
    }

    private static Column findColumnForName(String name, Set<Column> columns) {
        return columns.stream()
                .filter(c -> c.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No column for column name " + name));
    }

    @Override
    public Map<Column, SimplifiedIntervalList> getAllAvailableIntervals() {
        return new HashMap<>(this.availabilities);
    }

    @Override
    public TableName getTableName() {
        return name;
    }

    @Override
    public Set<DataSourceName> getDataSourceNames() {
        return dsNames;
    }

    @Override
    public String getPhysicalColumnName(String logicalName) {
        return logicalToPhysicalColumns.get(logicalName);
    }

    @Override
    public PhysicalTableSchema getSchema() {
        return schema;
    }

    @Override
    public String getName() {
        return name.asName();
    }

    @Override
    public DateTime getTableAlignment() {
        return Availability.DISTANT_PAST;
    }

    @Override
    public ConstrainedTable withConstraint(DataSourceConstraint constraint) {
        return new ConstrainedTable(this, constraint);
    }

    @Override
    public Availability getAvailability() {
        return availability;
    }

    /**
     * Sets the availability of this physical table.
     *
     * @param availability  Availability to set
     *
     * @deprecated This method is necessary because
     * {@link com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils} requires all tables to have this
     * public method, despite it not being on any interface. That contract needs to be made with either concrete or
     * refactored away.
     */
    @Deprecated
    public void setAvailability(Availability availability) {
        this.availability = availability;
    }

    /**
     * Availability implementation used by the {@code TestPhysicalTable}. Defers to the information on the encapsulating
     * physical table.
     */
    private class TestPhysicalTableAvailability implements Availability {

        private final Map<String, SimplifiedIntervalList> availabilities;

        /**
         * Constructor. Converts the mapping of column name to available intervals to a mapping of column to available
         * intervals to avoid performaing this calculation on each call to
         * {@link Availability#getAllAvailableIntervals()}.
         */
        private TestPhysicalTableAvailability() {
            this.availabilities = TestPhysicalTable.this.availabilities.entrySet().stream()
                    .map(entry ->
                            new AbstractMap.SimpleImmutableEntry<>(entry.getKey().getName(), entry.getValue())
                    )
                    .collect(Collectors.collectingAndThen(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    Map.Entry::getValue
                            ),
                            Collections::unmodifiableMap
                    ));
        }

        @Override
        public Set<DataSourceName> getDataSourceNames() {
            return TestPhysicalTable.this.getDataSourceNames();
        }

        @Override
        public Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
            return availabilities;
        }
    }
}
