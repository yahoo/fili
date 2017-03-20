package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.table.Column

import org.joda.time.DateTime
import org.joda.time.Interval
import org.joda.time.Period

import spock.lang.Specification

class ImmutableAvailabilitySpec extends Specification {

    List<Interval> lastYear = [new Interval(new DateTime("2015"), new Period("P1Y"))]

    Dimension dimension1 = Mock(Dimension) {
        getApiName() >> "d1"
    }
    Dimension dimension2 = Mock(Dimension) {
        getApiName() >> "d2"
    }

    Set<Dimension> dimensions = [dimension1, dimension2] as Set
    DimensionDictionary dictionary = new DimensionDictionary(dimensions)

    def "Empty segment data does not create an error"() {
        setup:
        Map<String, List<Interval>> segmentDimensionData = [:]
        Map<String, List<Interval>> segmentMetricData = [:]

        expect: "segment availability is passed to be bound to dimension and metric columns"
        (ImmutableAvailability.buildAvailabilityMap(segmentDimensionData, segmentMetricData, dictionary)).size() == 0
    }

    def "Availability loads and binds dimension and metric columns from String maps"() {
        setup: "Two configured dimensions are defined"
        List<Column> columns = dimensions.collect {new DimensionColumn(it)}

        and: "Segment metadata produces some configured and some unconfigured dimension names"
        Map<String, List<Interval>> segmentDimensionData = ["d1": lastYear, "d2": lastYear, "bad": lastYear]
        Map<String, List<Interval>> segmentMetricData = ["m1": lastYear, "m2": lastYear]
        List<MetricColumn> metricColumns = segmentMetricData.collect { new MetricColumn(it.getKey())}

        when: "segment availability is passed to be bound to dimension and metric columns"
        Map<Column, List<Interval>> result = ImmutableAvailability.buildAvailabilityMap(segmentDimensionData, segmentMetricData, dictionary)

        then: "all configured dimension columns and all metric columns now have associated availability"
        columns.every() {
            result.get(it) == lastYear
        }
        metricColumns.every() {
            result.get(it) == lastYear
        }
    }
}
