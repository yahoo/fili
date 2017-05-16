// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.config.metric.MetricInstance
import com.yahoo.bard.webservice.data.config.metric.makers.AggregationAverageMaker
import com.yahoo.bard.webservice.data.config.metric.makers.ArithmeticMaker
import com.yahoo.bard.webservice.data.config.metric.makers.LongSumMaker
import com.yahoo.bard.webservice.data.config.metric.makers.SketchCountMaker
import com.yahoo.bard.webservice.data.config.metric.makers.SketchSetOperationHelper
import com.yahoo.bard.webservice.data.config.metric.makers.SketchSetOperationMaker
import com.yahoo.bard.webservice.data.config.names.ApiMetricName
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
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggregation
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier
import com.yahoo.bard.webservice.druid.util.SketchFieldConverter
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.StrictPhysicalTable
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.TableGroup

import org.json.JSONArray
import org.json.JSONObject

import spock.lang.Specification
/**
 * This class is a resource container for intersection report tests.
 *
 * @deprecated  To conwizer the latest version of sketch Library.
 * This class is replaced by ThetaSketchIntersectionReportingResources class
 */
@Deprecated
class SketchIntersectionReportingResources extends Specification {
    public DimensionDictionary dimensionDict
    public MetricDictionary metricDict
    public LogicalTable table
    public JSONObject filterObj
    public PostAggregation fooPostAggregation;
    public Filter filter;
    public Set<FilteredAggregation> fooNoBarFilteredAggregationSet
    public Set<FilteredAggregation> fooRegFoosFilteredAggregationSet
    public SketchSetOperationPostAggregation fooNoBarPostAggregationInterim
    public Map<String, List<FilteredAggregation>> interimPostAggDictionary
    public SketchSetOperationPostAggregation fooRegFoosPostAggregationInterim
    public Aggregation fooNoBarAggregation
    public TemplateDruidQuery dayAvgFoosTdq
    public Dimension propertyDim
    public Dimension countryDim

    SketchIntersectionReportingResources init() {
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        //Initializing the Sketch field converters
        FieldConverterSupplier.sketchConverter = new SketchFieldConverter();
        FieldConverterSupplier.metricsFilterSetBuilder = new FilteredSketchMetricsHelper();

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


        //added dimensions to the physical table
        Set columns = [propertyDim, countryDim].collect() { new DimensionColumn(it)}

        // regFoos deliberately omitted
        Set<ApiMetricName> metrics = [buildMockName("foos"), buildMockName("fooNoBar"), buildMockName("pageViews"), buildMockName("bar"), buildMockName("wiz"), buildMockName("waz"), buildMockName("viz"), buildMockName("unregFoos"), buildMockName("ratioMetric")]
        //added metrics to the physical table
        columns.addAll( metrics.collect() { new MetricColumn(it.apiName)})

        metricDict = new MetricDictionary()

        SketchSetOperationMaker setUnionMaker = new SketchSetOperationMaker(
                metricDict,
                SketchSetOperationPostAggFunction.UNION
        )
        SketchCountMaker sketchCountMaker = new SketchCountMaker(new MetricDictionary(), 16384)
        ArithmeticMaker sumMaker = new ArithmeticMaker(metricDict, ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS);
        SketchSetOperationMaker setDifferenceMaker = new SketchSetOperationMaker(metricDict, SketchSetOperationPostAggFunction.NOT);
        AggregationAverageMaker simpleDailyAverageMaker = new AggregationAverageMaker(metricDict, DAY)

        MetricInstance pageViews = new MetricInstance("pageViews", new LongSumMaker(metricDict), "pageViews")
        MetricInstance fooNoBarInstance = new MetricInstance("fooNoBar", sketchCountMaker, "fooNoBar")
        MetricInstance regFoosInstance = new MetricInstance("regFoos", sketchCountMaker, "regFoos")
        MetricInstance foos = new MetricInstance("foos", setUnionMaker, "fooNoBar", "regFoos")
        MetricInstance dayAvgFoos = new MetricInstance("dayAvgFoos", simpleDailyAverageMaker, "foos")

        MetricInstance bar = new MetricInstance("bar", sketchCountMaker, "bar")
        MetricInstance wiz = new MetricInstance("wiz", sketchCountMaker, "wiz")
        MetricInstance waz = new MetricInstance("waz", sumMaker, "bar", "wiz")
        MetricInstance unregFoos = new MetricInstance("unregFoos", setDifferenceMaker, "fooNoBar", "regFoos")
        MetricInstance viz = new MetricInstance("viz", sumMaker, "waz", "unregFoos")

        metricDict.add(pageViews.make())
        metricDict.add(fooNoBarInstance.make())
        metricDict.add(regFoosInstance.make())
        metricDict.add(foos.make())
        metricDict.add(dayAvgFoos.make())

        metricDict.add(bar.make())
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
                "NETWORK",
                DAY.buildZonedTimeGrain(UTC),
                columns,
                [:],
                Mock(DataSourceMetadataService)
        )

        TableGroup tableGroup = new TableGroup([physicalTable] as LinkedHashSet, metrics, physicalTable.dimensions)
        table = new LogicalTable("NETWORK", DAY, tableGroup, metricDict)

        JSONArray metricJsonObjArray = new JSONArray("[{\"filter\":{\"AND\":\"country|id-in[US,IN],property|id-in[114,125]\"},\"name\":\"foo\"},{\"filter\":{},\"name\":\"pageviews\"}]")
        JSONObject jsonobject = metricJsonObjArray.getJSONObject(0)
        filterObj = jsonobject.getJSONObject("filter")

        fooPostAggregation = foos.make().templateDruidQuery.getPostAggregations().first()

        dayAvgFoosTdq = dayAvgFoos.make().templateDruidQuery;

        fooNoBarAggregation = fooNoBarInstance.make().templateDruidQuery.aggregations.first()
        Aggregation regFoosAggregation = regFoosInstance.make().templateDruidQuery.aggregations.first()

        fooNoBarFilteredAggregationSet = FieldConverterSupplier.metricsFilterSetBuilder.getFilteredAggregation(filterObj, fooNoBarAggregation, dimensionDict, table, new DataApiRequest())
        fooNoBarPostAggregationInterim = SketchSetOperationHelper.makePostAggFromAgg(SketchSetOperationPostAggFunction.INTERSECT, "fooNoBar", new ArrayList<>(fooNoBarFilteredAggregationSet))

        fooRegFoosFilteredAggregationSet = FieldConverterSupplier.metricsFilterSetBuilder.getFilteredAggregation(filterObj, regFoosAggregation, dimensionDict, table, new DataApiRequest())
        fooRegFoosPostAggregationInterim = SketchSetOperationHelper.makePostAggFromAgg(SketchSetOperationPostAggFunction.INTERSECT, "regFoos", new ArrayList<>(fooRegFoosFilteredAggregationSet))

        interimPostAggDictionary = [:]
        interimPostAggDictionary.put(fooNoBarAggregation.getName(), fooNoBarFilteredAggregationSet as List)
        interimPostAggDictionary.put(regFoosAggregation.getName(), fooRegFoosFilteredAggregationSet as List)

        return this
    }

    ApiMetricName buildMockName(String name) {
        return ApiMetricName.of(name)
    }
}
