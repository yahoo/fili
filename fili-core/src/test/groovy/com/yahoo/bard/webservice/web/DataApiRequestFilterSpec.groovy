// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.config.BardFeatureFlag.DATA_FILTER_SUBSTRING_OPERATIONS
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.util.FilterTokenizer
import com.yahoo.bard.webservice.util.IntervalUtils
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.apirequest.generator.filter.FilterBinders

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class DataApiRequestFilterSpec extends Specification {

    @Shared
    DimensionDictionary dimensionDict
    @Shared
    MetricDictionary metricDict
    @Shared
    LogicalTable table

    static final DateTimeZone originalTimeZone = DateTimeZone.default

    def setupSpec() {
        DateTimeZone.default = IntervalUtils.SYSTEM_ALIGNMENT_EPOCH.zone
    }

    def setup() {
        LinkedHashSet dimensionFields = [BardDimensionField.ID, BardDimensionField.DESC]

        dimensionDict = new DimensionDictionary()
        KeyValueStoreDimension keyValueStoreDimension
        [ "locale", "one", "two", "three" ].each { String name ->
            keyValueStoreDimension = new KeyValueStoreDimension(name, "druid-"+name, dimensionFields, MapStoreManager.getInstance(name), ScanSearchProviderManager.getInstance(name))
            keyValueStoreDimension.setLastUpdated(new DateTime(10000))
            dimensionDict.add(keyValueStoreDimension)
        }

        metricDict = new MetricDictionary()
        [ "met1", "met2", "met3", "met4" ].each { String name ->
            metricDict.put(name, new LogicalMetricImpl(null, null, name))
        }
        TableGroup tg = Mock(TableGroup)
        tg.getApiMetricNames() >> ([] as Set)
        tg.getDimensions() >> dimensionDict.apiNameToDimension.values()
        table = new LogicalTable("name", DAY, tg, metricDict)
    }

    def cleanupSpec() {
        DateTimeZone.default = originalTimeZone
    }

    @Unroll
    def "Find #filterCount filters and #filterValueCount values when parsing filter string #filterString"() {
        when:
        Map<Dimension, Set<ApiFilter>> filters = FilterBinders.instance.generateFilters(
                filterString,
                table,
                dimensionDict
        )

        then:
        filters.size() == dimensions
        filters.values().flatten().size() == filterCount
        filters.values().flatten()*.getValues().flatten().size() == filterValueCount

        where:
        dimensions | filterCount | filterValueCount | filterString
        1          | 1           | 1                | "locale|desc-in[US]"
        1          | 1           | 2                | "locale|desc-in[US,India]"
        1          | 1           | 3                | "locale|desc-in[US,India,Canada]"
        1          | 2           | 2                | "locale|desc-in[US],locale|id-in[5]"
        1          | 2           | 3                | "locale|desc-in[US,India],locale|id-in[5]"
        1          | 2           | 4                | "locale|desc-in[US,India],locale|id-in[5,8]"
        1          | 2           | 4                | "locale|desc-in[US,India],locale|id-eq[5,8]"
        1          | 2           | 4                | "locale|desc-notin[US,India],locale|id-in[5,8]"
        2          | 3           | 3                | "locale|desc-in[US],locale|id-eq[5],one|id-in[blue]"
        2          | 3           | 6                | "locale|desc-notin[US,India],locale|id-eq[5,8],one|id-in[blue,red]"
        2          | 3           | 6                | "locale|desc-notin[US,India],locale|id-eq[5,8],one|id-in[US,India]"
        2          | 3           | 6                | "locale|desc-contains[US,India],locale|id-eq[5,8],one|id-in[US,India]"
        2          | 3           | 6                | "locale|desc-startswith[US,India],locale|id-eq[5,8],one|id-in[US,India]"
    }

    def "Error thrown when startswith and contains feature flag is off, but filter has #startsWithContains" () {
        given: "The feature flag for startsWith and contains is turned off"
        boolean originalFeatureFlagSetting = DATA_FILTER_SUBSTRING_OPERATIONS.isOn()
        DATA_FILTER_SUBSTRING_OPERATIONS.setOn(false)

        when: "We try to generate the filter"
        FilterBinders.instance.generateFilters(filterString, table, dimensionDict)

        then: "An error is thrown"
        thrown(BadApiRequestException)

        cleanup:
        DATA_FILTER_SUBSTRING_OPERATIONS.setOn(originalFeatureFlagSetting)

        where:
        startsWithContains << ["startswith", "contains"]
        filterString = "locale|desc-$startsWithContains[US,India],locale|id-eq[5,8],one|id-in[US,India]"

    }

    def "check invalid field for dimension creates error"() {
        setup:
        String expectedMessage = ErrorMessageFormat.FILTER_FIELD_NOT_IN_DIMENSIONS.format('unknown', 'locale')
        when:
        FilterBinders.instance.generateFilters(
                "locale|unknown-in[US,India],locale|id-eq[5]",
                table,
                dimensionDict
        )

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }

    def "check dimension not in logical table creates error"() {
        setup:
        TableGroup tg = Mock(TableGroup)
        tg.getDimensions() >> ([] as Set)
        tg.getApiMetricNames() >> ([] as Set)
        table = new LogicalTable("name", DAY, tg, metricDict)

        String expectedMessage = ErrorMessageFormat.FILTER_DIMENSION_NOT_IN_TABLE.format('locale', 'name')
        when:
        FilterBinders.instance.generateFilters(
                "locale|id-in[US,India],locale|id-eq[5]",
                table,
                dimensionDict
        )

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }

    def "check dimension doesn't exist creates error"() {
        setup:
        String expectedMessage = ErrorMessageFormat.FILTER_DIMENSION_UNDEFINED.format('undefined')
        when:
        FilterBinders.instance.generateFilters(
                "undefined|id-in[US,India],locale|id-eq[5]",
                table,
                dimensionDict
        )

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }

    def "check invalid syntax creates error"() {
        setup:
        // Split for filter splits to ],.  Everything before this is included in bad error.
        String expectedMessage = ErrorMessageFormat.FILTER_INVALID.format('locale.id-in[US,India]')
        when:
        FilterBinders.instance.generateFilters(
                "locale.id-in[US,India],locale.id-eq[5]",
                table,
                dimensionDict
        )

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }

    def "check invalid field value creates error"() {
        setup:
        String filter = "locale|id-in[,India]"
        // Split for filter splits to ],.  Everything before this is included in bad error.
        String error = String.format(FilterTokenizer.PARSING_FAILURE_UNQOUTED_VALUES_FORMAT, ',India')

        String expectedMessage = ErrorMessageFormat.FILTER_ERROR.format(filter, error)
        when:
        FilterBinders.instance.generateFilters(filter, table, dimensionDict)

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }

    def "check invalid operator creates error"() {
        setup:
        // Split for filter splits to ],.  Everything before this is included in bad error.
        String expectedMessage = ErrorMessageFormat.FILTER_OPERATOR_INVALID.format('in:')
        when:
        FilterBinders.instance.generateFilters(
                "locale|id-in:[US,India],locale|id-eq[5]",
                table,
                dimensionDict
        )

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }
}
