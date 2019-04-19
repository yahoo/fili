// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.config.metric.MetricInstance
import com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchMaker
import com.yahoo.bard.webservice.data.config.names.ApiMetricName
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder
import com.yahoo.bard.webservice.druid.model.builders.DruidOrFilterBuilder
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.physicaltables.StrictPhysicalTable
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.FilteredThetaSketchMetricsHelper
import com.yahoo.bard.webservice.web.MetricsFilterSetBuilder
import com.yahoo.bard.webservice.web.apirequest.binders.FilterBinders

import org.apache.commons.lang3.tuple.Pair

import spock.lang.Specification

class FilteredAggregationSpec extends Specification{

    FilteredAggregation filteredAgg
    FilteredAggregation filteredAgg2
    Filter filter1
    Filter filter2
    Aggregation metricAgg
    Aggregation genderDependentMetricAgg
    KeyValueStoreDimension ageDimension
    KeyValueStoreDimension genderDimension
    static MetricsFilterSetBuilder oldBuilder = FieldConverterSupplier.metricsFilterSetBuilder

    FilterBinders filterBinders = FilterBinders.instance

    def setupSpec() {
        FieldConverterSupplier.metricsFilterSetBuilder = new FilteredThetaSketchMetricsHelper()
    }

    def setup() {
        MetricDictionary metricDictionary = new MetricDictionary()

        def filtered_metric_name = "FOO_NO_BAR"
        Set<ApiMetricName> metricNames = (["FOO", filtered_metric_name].collect { ApiMetricName.of(it)}) as Set

        ageDimension = buildSimpleDimension("age")
        genderDimension = buildSimpleDimension("gender")

        DimensionDictionary dimensionDictionary = new DimensionDictionary([ageDimension] as Set)

        ageDimension.addDimensionRow(BardDimensionField.makeDimensionRow(ageDimension, "114"))
        ageDimension.addDimensionRow(BardDimensionField.makeDimensionRow(ageDimension, "125"))

        Set<Column> columns = [new DimensionColumn(ageDimension)] as Set

        PhysicalTable physicalTable = new StrictPhysicalTable(
                TableName.of("NETWORK"),
                DAY.buildZonedTimeGrain(UTC),
                columns,
                [:],
                Mock(DataSourceMetadataService)
        )

        ThetaSketchMaker sketchCountMaker = new ThetaSketchMaker(new MetricDictionary(), 16384)
        MetricInstance fooNoBarSketchPm = new MetricInstance(filtered_metric_name,sketchCountMaker,"FOO_NO_BAR_SKETCH")
        LogicalMetric fooNoBarSketch = fooNoBarSketchPm.make()
        metricDictionary.put(filtered_metric_name, fooNoBarSketch)

        metricAgg = fooNoBarSketch.getTemplateDruidQuery().getAggregations().first()
        genderDependentMetricAgg = Mock(Aggregation)
        genderDependentMetricAgg.getDependentDimensions() >> ([genderDimension] as Set)
        genderDependentMetricAgg.withName(_) >> genderDependentMetricAgg
        genderDependentMetricAgg.withFieldName(_) >> genderDependentMetricAgg

        Set<ApiFilter> filterSet = [filterBinders.generateApiFilter("age|id-in[114,125]", dimensionDictionary)] as Set

        DruidFilterBuilder filterBuilder = new DruidOrFilterBuilder()
        filter1  = filterBuilder.buildFilters([(ageDimension): filterSet])

        filter2 = filterBuilder.buildFilters(
                [(ageDimension): [filterBinders.generateApiFilter("age|id-in[114]", dimensionDictionary)] as Set]
        )

        filteredAgg = new FilteredAggregation("FOO_NO_BAR-114_127", metricAgg, filter1)
        filteredAgg2 = new FilteredAggregation("FOO_NO_BAR-114_127", genderDependentMetricAgg, filter1)
    }

    def cleanupSpec() {
        FieldConverterSupplier.metricsFilterSetBuilder = oldBuilder
    }

    def "test the filtered aggregator constructor" (){
        expect:
        filteredAgg.getFieldName() == "FOO_NO_BAR_SKETCH"
        filteredAgg.getName() == "FOO_NO_BAR-114_127"
        filteredAgg.getFilter() == filter1
        filteredAgg.getAggregation() == metricAgg.withName("FOO_NO_BAR-114_127")
    }

    def "test FilteredAggregation withFieldName method" (){
        FilteredAggregation newFilteredAggregation = filteredAgg.withFieldName("NEW_FIELD_NAME");

        expect:
        newFilteredAggregation.getFieldName() == "NEW_FIELD_NAME"
        newFilteredAggregation.getName() == filteredAgg.getName()
        newFilteredAggregation.getFilter() == filteredAgg.getFilter()
        newFilteredAggregation.getAggregation() == metricAgg.withName(filteredAgg.getName()).withFieldName("NEW_FIELD_NAME")
    }

    def "test FilteredAggregation withName method" (){
        FilteredAggregation newFilteredAggregation = filteredAgg.withName("FOO_NO_BAR-US_IN");

        expect:
        newFilteredAggregation.getFieldName() == filteredAgg.getFieldName()
        newFilteredAggregation.getName() == "FOO_NO_BAR-US_IN"
        newFilteredAggregation.getFilter() == filteredAgg.getFilter()
        newFilteredAggregation.getAggregation() == metricAgg.withName("FOO_NO_BAR-US_IN").withFieldName(filteredAgg.getFieldName())
    }

    def "test FilteredAggregation withFilter method" (){
        FilteredAggregation newFilteredAggregation = filteredAgg.withFilter(filter2);

        expect:
        newFilteredAggregation.getFieldName() == filteredAgg.getFieldName()
        newFilteredAggregation.getName() == filteredAgg.getName()
        newFilteredAggregation.getFilter() == filter2
        newFilteredAggregation.getAggregation() == metricAgg.withName(filteredAgg.getName()).withFieldName(filteredAgg.getFieldName())
    }

    def "test FilteredAggregation withAggregation method" (){
        FilteredAggregation newFilteredAggregation = filteredAgg.withAggregation(metricAgg.withName("NEW_AGG").
                withFieldName("NEW_FIELD_NAME"));

        expect:
        newFilteredAggregation.getFieldName() == "NEW_FIELD_NAME"
        newFilteredAggregation.getName() == "NEW_AGG"
        newFilteredAggregation.getFilter() == filteredAgg.getFilter()
        newFilteredAggregation.getAggregation() == metricAgg.withName("NEW_AGG").
                withFieldName("NEW_FIELD_NAME")
    }

    def "Test dependant metric"() {
        expect:
        filteredAgg.dependentDimensions == [ageDimension] as Set
        filteredAgg2.dependentDimensions == [genderDimension, ageDimension] as Set
    }

    def "test serialization" (){
        expect:
        filteredAgg.getAggregation().getFieldName() == "FOO_NO_BAR_SKETCH"
        filteredAgg.getAggregation().getName() == "FOO_NO_BAR-114_127"
    }

    def "test nesting pushes filter to bottom"() {
        setup:
        Pair<Optional<Aggregation>, Optional<Aggregation>> baseExpectedNestedAggs = filteredAgg.getAggregation().nest()

        when:
        Pair<Optional<Aggregation>, Optional<Aggregation>> nestedAggs = filteredAgg.nest()
        Aggregation inner = nestedAggs.getRight().get()
        Aggregation outer = nestedAggs.getLeft().get()

        then:
        inner instanceof FilteredAggregation
        inner.getType() == "filtered"
        ((FilteredAggregation) inner).getFilter() == filter1
        inner.getName() == baseExpectedNestedAggs.getRight().get().getName()
        inner.getFieldName() == baseExpectedNestedAggs.getRight().get().getFieldName()

        and:
        outer instanceof ThetaSketchAggregation
        outer.getType() == baseExpectedNestedAggs.getLeft().get().getType()
        outer.getName() == baseExpectedNestedAggs.getLeft().get().getName()
        outer.getFieldName() == baseExpectedNestedAggs.getLeft().get().getFieldName()
    }

    def Dimension buildSimpleDimension(String name) {
        return new KeyValueStoreDimension(
                name,
                null,
                [BardDimensionField.ID] as LinkedHashSet,
                MapStoreManager.getInstance(name),
                ScanSearchProviderManager.getInstance(name)
        )

    }
}
