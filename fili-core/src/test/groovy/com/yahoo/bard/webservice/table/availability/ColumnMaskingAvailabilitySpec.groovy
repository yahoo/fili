// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.metadata.TestDimension
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.PhysicalTableSchema
import com.yahoo.bard.webservice.table.resolver.BaseDataSourceConstraint
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList
import com.yahoo.bard.webservice.web.filters.ApiFilters

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import spock.lang.Specification
import sun.util.locale.provider.AvailableLanguageTags

class ColumnMaskingAvailabilitySpec extends Specification {

    ConstraintTrackingAvailability testAvailability
    DataSourceConstraint testConstraint
    ColumnMaskingAvailability maskedAvailability

    Dimension filter1
    Dimension filter2
    Dimension other1
    Dimension other2

    String filter1Name
    String filter2Name
    String other1Name
    String other2Name

    def setup() {
        filter1Name = "filter1"
        filter1 = Mock(Dimension)
        filter1.getApiName() >> filter1Name

        filter2Name = "filter2"
        filter2 = Mock(Dimension)
        filter2.getApiName() >> filter2Name

        other1Name = "other1"
        other1 = Mock(Dimension)
        other1.getApiName() >> other1Name

        other2Name = "other2"
        other2 = Mock(Dimension)
        other2.getApiName() >> other2Name


        testConstraint = new BaseDataSourceConstraint(
                [other1, filter1] as Set,
                [other2, filter2] as Set,
                [] as Set,
                [] as Set,
                new ApiFilters()
        )
        testAvailability = new ConstraintTrackingAvailability(filter1Name, filter2Name)
        maskedAvailability = new ColumnMaskingAvailability(testAvailability, [filter1Name, filter2Name] as Set)
    }

    def "all constrained availability methods have filtered dimensions removed from constraint"() {
        when:
        testAvailability.clearConstraintDimensions()
        maskedAvailability.getDataSourceNames(testConstraint)

        then:
        testAvailability.constraintDimensions == [other1, other2] as Set

        when:
        testAvailability.clearConstraintDimensions()
        maskedAvailability.getAvailableIntervals(testConstraint)

        then:
        testAvailability.constraintDimensions == [other1, other2] as Set

        when:
        testAvailability.clearConstraintDimensions()
        maskedAvailability.getExpectedStartDate(testConstraint)

        then:
        testAvailability.constraintDimensions == [other1, other2] as Set

        when:
        testAvailability.clearConstraintDimensions()
        maskedAvailability.getExpectedEndDate(testConstraint)

        then:
        testAvailability.constraintDimensions == [other1, other2] as Set
    }
    def "Test column masking when constrained by  physical constraint"() {
        setup:
        String missingDimensionPhysicalName = "missing_dimension"
        String missingDimensionApiName = "missingDimension"

        AvailabilityTest target = new AvailabilityTest()
        //build map of druid physical columns and their available time intervals
        target.physicalAvailabity = [
                column_1 : new SimplifiedIntervalList([ new Interval("2020-01-01/2020-03-01")]),
                column_2 : new SimplifiedIntervalList([ new Interval("2020-02-01/2020-03-01")]),
                (missingDimensionPhysicalName) : new SimplifiedIntervalList(),
        ]

        //configure Dimensions and Metrics
        TestDimension filteredDimension = new TestDimension(missingDimensionApiName)
        TestDimension dimensionOne = new TestDimension("dimensionOne")

        LogicalMetric metricOne = Mock(LogicalMetric)
        metricOne.getName() >> "metricOne"

        ColumnMaskingAvailability test = new ColumnMaskingAvailability(target, [missingDimensionApiName] as Set)

        Iterable<Column> cols = [ new Column("column_1"), new Column("column_2"), new Column(missingDimensionPhysicalName)]

        //Mapping of  logical dimensions to physical druid
        Map<String, String> logicalToPhysicalNamesMapping = [
                dimensionOne : "column_1" ,
                metricOne : "column_2" ,
                (missingDimensionApiName) : missingDimensionPhysicalName
        ]

        //Build physical table schema
        PhysicalTableSchema schema = new PhysicalTableSchema(
                new ZonedTimeGrain(DefaultTimeGrain.DAY, DateTimeZone.UTC),
                cols,
                logicalToPhysicalNamesMapping
        )

        DataSourceConstraint baseConstraint = new BaseDataSourceConstraint(
                [dimensionOne] as Set,
                [filteredDimension] as Set,
                [] as Set,
                ["metricOne"] as Set,
                new ApiFilters()
        )

        PhysicalDataSourceConstraint constraint = new PhysicalDataSourceConstraint(baseConstraint , schema)

        when:
        SimplifiedIntervalList result = test.getAvailableIntervals(constraint)

        then:
        result.getFirst() == ["2020-02-01/2020-03-01"] as Interval
    }

    class AvailabilityTest implements Availability{
        Map<String, SimplifiedIntervalList> physicalAvailabity = [:]

        @Override
        Set<DataSourceName> getDataSourceNames() {
            return "TestAvailablityOnDruidColumns"
        }

        @Override
        Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
            return [:]
        }

        @Override
        SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
            if(! (constraint instanceof PhysicalDataSourceConstraint)){
                throw new IllegalArgumentException("Exception : this is only for physical constraints test")
            }
            PhysicalDataSourceConstraint physicalDataSource = (PhysicalDataSourceConstraint) constraint

            SimplifiedIntervalList intrvls = null
            for(Map.Entry<String, SimplifiedIntervalList> entry : physicalAvailabity.entrySet()){
                if(!physicalDataSource.getAllColumnPhysicalNames().contains(entry.getKey()))
                        continue;
                if(intrvls == null){
                    intrvls = entry.getValue()
                }
                else{
                    intrvls = intrvls.intersect(entry.getValue())
                }

            }
            return intrvls
        }
    }

    class ConstraintTrackingAvailability implements Availability {
        final Set<String> filteredColumnNames
        Set<Dimension> constraintDimensions

        ConstraintTrackingAvailability(String ... filteredColumnNames) {
            this.filteredColumnNames = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(filteredColumnNames)))
        }

        void clearConstraintDimensions() {
            constraintDimensions = [] as Set
        }

        @Override
        Set<DataSourceName> getDataSourceNames() {
            return [DataSourceName.of("dsName")]
        }

        @Override
        Set<DataSourceName> getDataSourceNames(DataSourceConstraint constraint) {
            constraintDimensions = new HashSet<>(constraint.getAllDimensions())
            return getDataSourceNames()
        }

        @Override
        Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
            return [:]
        }

        @Override
        SimplifiedIntervalList getAvailableIntervals() {
            return new SimplifiedIntervalList()
        }

        @Override
        SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
            constraintDimensions = new HashSet<>(constraint.getAllDimensions())
            return getAvailableIntervals()
        }

        @Override
        Optional<DateTime> getExpectedStartDate(DataSourceConstraint constraint) {
            constraintDimensions = new HashSet<>(constraint.getAllDimensions())
            return Optional.empty()
        }

        @Override
        Optional<DateTime> getExpectedEndDate(DataSourceConstraint constraint) {
            constraintDimensions = new HashSet<>(constraint.getAllDimensions())
            return Optional.empty()
        }
    }
}
