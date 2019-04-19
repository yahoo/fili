// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.config.metric.MetricInstance
import com.yahoo.bard.webservice.data.config.metric.makers.AggregationAverageMaker
import com.yahoo.bard.webservice.data.config.metric.makers.ArithmeticMaker
import com.yahoo.bard.webservice.data.config.metric.makers.LongSumMaker
import com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchMaker
import com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchSetOperationHelper
import com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchSetOperationMaker
import com.yahoo.bard.webservice.data.config.names.ApiMetricName
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricColumn
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.InFilter
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchSetOperationPostAggregation
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier
import com.yahoo.bard.webservice.druid.util.ThetaSketchFieldConverter
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.physicaltables.StrictPhysicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.web.apirequest.utils.TestingDataApiRequestImpl

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode

import spock.lang.Specification
/**
 * This class is a resource container for intersection report tests.
 */
class ThetaSketchIntersectionReportingResources extends Specification {
    public DimensionDictionary dimensionDict
    public MetricDictionary metricDict
    public LogicalTable table
    public PostAggregation fooPostAggregation
    public Filter filter
    public JsonNode filterObj
    public Set<FilteredAggregation> fooNoBarFilteredAggregationSet
    public Set<FilteredAggregation> fooRegFoosFilteredAggregationSet
    public ThetaSketchSetOperationPostAggregation fooNoBarPostAggregationInterim
    public Map<String, List<FilteredAggregation>> interimPostAggDictionary
    public ThetaSketchSetOperationPostAggregation fooRegFoosPostAggregationInterim
    public Aggregation fooNoBarAggregation
    public TemplateDruidQuery dayAvgFoosTdq
    public Dimension propertyDim
    public Dimension countryDim
    public ObjectMapper mapper

    ThetaSketchIntersectionReportingResources init() {
        mapper = new ObjectMappersSuite().getMapper()

        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        //Initializing the Sketch field converters
        FieldConverterSupplier.sketchConverter = new ThetaSketchFieldConverter();
        FieldConverterSupplier.metricsFilterSetBuilder = new FilteredThetaSketchMetricsHelper();

        propertyDim = new KeyValueStoreDimension(
                "property",
                null,
                [BardDimensionField.ID] as LinkedHashSet,
                MapStoreManager.getInstance("property"),
                ScanSearchProviderManager.getInstance("property")
        )

        countryDim = new KeyValueStoreDimension(
                "country",
                null,
                [BardDimensionField.ID] as LinkedHashSet,
                MapStoreManager.getInstance("country"),
                ScanSearchProviderManager.getInstance("country")
        )

        propertyDim.addDimensionRow(BardDimensionField.makeDimensionRow(propertyDim, "114"))
        propertyDim.addDimensionRow(BardDimensionField.makeDimensionRow(propertyDim, "125"))

        countryDim.addDimensionRow(BardDimensionField.makeDimensionRow(countryDim, "US"))
        countryDim.addDimensionRow(BardDimensionField.makeDimensionRow(countryDim, "IN"))


        dimensionDict = new DimensionDictionary()
        dimensionDict.addAll([propertyDim, countryDim])
        //Reg foos omitted to make invalid on table
        Set<ApiMetricName> metrics = [buildMockName("foos"), buildMockName("fooNoBar"), buildMockName("pageViews"), buildMockName("foo"), buildMockName("wiz"), buildMockName("waz"), buildMockName("viz"), buildMockName("unregFoos"), buildMockName("ratioMetric")]

        Set<Column> columns = (Set<? extends Column>) (metrics.collect {
            new MetricColumn(it.apiName)
        }.toSet())

        columns.add(new DimensionColumn(propertyDim))
        columns.add(new DimensionColumn(countryDim))


        metricDict = new MetricDictionary()

        ThetaSketchSetOperationMaker setUnionMaker = new ThetaSketchSetOperationMaker(
                metricDict,
                SketchSetOperationPostAggFunction.UNION
        )
        ThetaSketchMaker ThetaSketchMaker = new ThetaSketchMaker(new MetricDictionary(), 16384)
        ArithmeticMaker sumMaker = new ArithmeticMaker(metricDict, ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS);
        ThetaSketchSetOperationMaker setDifferenceMaker = new ThetaSketchSetOperationMaker(metricDict, SketchSetOperationPostAggFunction.NOT);
        AggregationAverageMaker simpleDailyAverageMaker = new AggregationAverageMaker(metricDict, DAY)

        MetricInstance pageViews = new MetricInstance("pageViews", new LongSumMaker(metricDict), "pageViews")
        MetricInstance fooNoBarInstance = new MetricInstance("fooNoBar", ThetaSketchMaker, "fooNoBar")
        MetricInstance regFoosInstance = new MetricInstance("regFoos", ThetaSketchMaker, "regFoos")
        MetricInstance foos = new MetricInstance("foos", setUnionMaker, "fooNoBar", "regFoos")
        MetricInstance dayAvgFoos = new MetricInstance("dayAvgFoos", simpleDailyAverageMaker, "foos")

        MetricInstance foo = new MetricInstance("foo", ThetaSketchMaker, "foo")
        MetricInstance wiz = new MetricInstance("wiz", ThetaSketchMaker, "wiz")
        MetricInstance waz = new MetricInstance("waz", sumMaker, "foo", "wiz")
        MetricInstance unregFoos = new MetricInstance("unregFoos", setDifferenceMaker, "fooNoBar", "regFoos")
        MetricInstance viz = new MetricInstance("viz", sumMaker, "waz", "unregFoos")

        metricDict.add(pageViews.make())
        metricDict.add(fooNoBarInstance.make())
        metricDict.add(regFoosInstance.make())
        metricDict.add(foos.make())
        metricDict.add(dayAvgFoos.make())

        metricDict.add(foo.make())
        metricDict.add(wiz.make())
        metricDict.add(waz.make())
        metricDict.add(unregFoos.make())
        metricDict.add(viz.make())

        LogicalMetric foosMetric = metricDict.get("foos")
        LogicalMetric ratioMetric = new LogicalMetric(foosMetric.templateDruidQuery, foosMetric.calculation, "ratioMetric", "ratioMetric Long Name", "Ratios", "Dummy metric Ratio Metric description")
        metricDict.add(ratioMetric)

        LogicalMetricColumn lmc = new LogicalMetricColumn(foosMetric);

        columns.add(lmc)

        PhysicalTable physicalTable = new StrictPhysicalTable(
                TableName.of("NETWORK"),
                DAY.buildZonedTimeGrain(UTC),
                columns,
                ["property": "property", "country": "country"],
                Mock(DataSourceMetadataService)
        )

        TableGroup tableGroup = new TableGroup([physicalTable] as LinkedHashSet, metrics, physicalTable.dimensions)

        table = new LogicalTable("NETWORK", DAY, tableGroup, metricDict)

        ArrayNode metricJsonObjArray = mapper.readTree("[{\"filter\":{\"AND\":\"country|id-in[US,IN],property|id-in[114,125]\"},\"name\":\"foo\"},{\"filter\":{},\"name\":\"pageviews\"}]")
        JsonNode jsonobject = metricJsonObjArray.get(0)
        filterObj = jsonobject.get("filter")

        filter = new InFilter(countryDim, ["US", "IN"])

        fooPostAggregation = foos.make().templateDruidQuery.getPostAggregations().first()

        dayAvgFoosTdq = dayAvgFoos.make().templateDruidQuery;

        fooNoBarAggregation = fooNoBarInstance.make().templateDruidQuery.aggregations.first()
        Aggregation regFoosAggregation = regFoosInstance.make().templateDruidQuery.aggregations.first()

        fooNoBarFilteredAggregationSet = FieldConverterSupplier.metricsFilterSetBuilder.getFilteredAggregation(
                filterObj,
                fooNoBarAggregation,
                dimensionDict,
                table,
                new TestingDataApiRequestImpl()
        )
        fooNoBarPostAggregationInterim = ThetaSketchSetOperationHelper.makePostAggFromAgg(
                SketchSetOperationPostAggFunction.INTERSECT,
                "fooNoBar",
                new ArrayList<>(fooNoBarFilteredAggregationSet)
        )

        fooRegFoosFilteredAggregationSet = FieldConverterSupplier.metricsFilterSetBuilder.getFilteredAggregation(
                filterObj,
                regFoosAggregation,
                dimensionDict,
                table,
                new TestingDataApiRequestImpl()
        )
        fooRegFoosPostAggregationInterim = ThetaSketchSetOperationHelper.makePostAggFromAgg(
                SketchSetOperationPostAggFunction.INTERSECT,
                "regFoos",
                new ArrayList<>(fooRegFoosFilteredAggregationSet)
        )

        interimPostAggDictionary = [:]
        interimPostAggDictionary.put(fooNoBarAggregation.getName(), fooNoBarFilteredAggregationSet as List)
        interimPostAggDictionary.put(regFoosAggregation.getName(), fooRegFoosFilteredAggregationSet as List)

        return this
    }

    ApiMetricName buildMockName(String name) {
        return ApiMetricName.of(name)
    }
}
