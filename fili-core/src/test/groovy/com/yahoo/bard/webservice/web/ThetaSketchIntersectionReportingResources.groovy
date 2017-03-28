// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.config.metric.MetricInstance
import com.yahoo.bard.webservice.data.config.metric.makers.AggregationAverageMaker
import com.yahoo.bard.webservice.data.config.metric.makers.ArithmeticMaker
import com.yahoo.bard.webservice.data.config.metric.makers.LongSumMaker
import com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchMaker
import com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchSetOperationHelper
import com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchSetOperationMaker
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
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchSetOperationPostAggregation
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier
import com.yahoo.bard.webservice.druid.util.ThetaSketchFieldConverter
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.ConcretePhysicalTable
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.TableGroup

import org.json.JSONArray
import org.json.JSONObject

import spock.lang.Specification

/**
 * This class is a resource container for intersection report tests.
 */
class ThetaSketchIntersectionReportingResources extends Specification {
    public DimensionDictionary dimensionDictionary
    public MetricDictionary metricDictionary
    public LogicalTable table
    public JSONObject filterObj
    public PostAggregation fooPostAggregation;
    public Filter filter;
    public Set<FilteredAggregation> fooNoBarFilteredAggregationSet
    public Set<FilteredAggregation> fooRegFoosFilteredAggregationSet
    public ThetaSketchSetOperationPostAggregation fooNoBarPostAggregationInterim
    public Map<String, List<FilteredAggregation>> interimPostAggDictionary
    public ThetaSketchSetOperationPostAggregation fooRegFoosPostAggregationInterim
    public Aggregation fooNoBarAggregation
    public TemplateDruidQuery dayAvgFoosTdq
    public Dimension propertyDim
    public Dimension countryDim

    public static final ApiMetricName PAGE_VIEWS = ApiMetricName.of("pageViews")
    public static final ApiMetricName FOO_NO_BAR = ApiMetricName.of("fooNoBar")
    public static final ApiMetricName FOOS = ApiMetricName.of("foos")
    public static final ApiMetricName FOO = ApiMetricName.of("foo")
    public static final ApiMetricName REG_FOOS = ApiMetricName.of("regFoos")
    public static final ApiMetricName DAY_AVG_FOOS = ApiMetricName.of("dayAvgFoos")
    public static final ApiMetricName UNREG_FOOS = ApiMetricName.of("unregFoos")
    public static final ApiMetricName WIZ = ApiMetricName.of("wiz")
    public static final ApiMetricName WAZ = ApiMetricName.of("waz")
    public static final ApiMetricName VIZ = ApiMetricName.of("viz")
    public static final ApiMetricName RATIO_METRIC = ApiMetricName.of("ratioMetric")

    ThetaSketchIntersectionReportingResources init() {
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


        dimensionDictionary = new DimensionDictionary()
        dimensionDictionary.addAll([propertyDim, countryDim])
        //Reg foos omitted to make invalid on table
        Set<ApiMetricName> metrics = [FOOS, FOO_NO_BAR, PAGE_VIEWS, FOO, WIZ, WAZ, VIZ, UNREG_FOOS, RATIO_METRIC]

        Set<Column> columns = (Set<? extends Column>) (metrics.collect {
            new MetricColumn(it.apiName)
        }.toSet())

        columns.add(new DimensionColumn(propertyDim))
        columns.add(new DimensionColumn(countryDim))


        metricDictionary = new MetricDictionary()

        ThetaSketchSetOperationMaker setUnionMaker = new ThetaSketchSetOperationMaker(
                metricDictionary,
                SketchSetOperationPostAggFunction.UNION
        )
        ThetaSketchMaker ThetaSketchMaker = new ThetaSketchMaker(new MetricDictionary(), 16384)
        ArithmeticMaker sumMaker = new ArithmeticMaker(metricDictionary, ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS);
        ThetaSketchSetOperationMaker setDifferenceMaker = new ThetaSketchSetOperationMaker(metricDictionary, SketchSetOperationPostAggFunction.NOT);
        AggregationAverageMaker simpleDailyAverageMaker = new AggregationAverageMaker(metricDictionary, DAY)

        MetricInstance pageViews = new MetricInstance(PAGE_VIEWS, new LongSumMaker(metricDictionary), PAGE_VIEWS)
        MetricInstance fooNoBarInstance = new MetricInstance(FOO_NO_BAR, ThetaSketchMaker, FOO_NO_BAR)
        MetricInstance regFoosInstance = new MetricInstance(REG_FOOS, ThetaSketchMaker, REG_FOOS)
        MetricInstance foos = new MetricInstance(FOOS, setUnionMaker, FOO_NO_BAR, REG_FOOS)
        MetricInstance dayAvgFoos = new MetricInstance(DAY_AVG_FOOS, simpleDailyAverageMaker, FOOS)

        MetricInstance foo = new MetricInstance(FOO, ThetaSketchMaker, FOO)
        MetricInstance wiz = new MetricInstance(WIZ, ThetaSketchMaker, WIZ)
        MetricInstance waz = new MetricInstance(WAZ, sumMaker, FOO, WIZ)
        MetricInstance unregFoos = new MetricInstance(UNREG_FOOS, setDifferenceMaker, FOO_NO_BAR, REG_FOOS)
        MetricInstance viz = new MetricInstance(VIZ, sumMaker, WAZ, UNREG_FOOS)

        metricDictionary.add(pageViews.make())
        metricDictionary.add(fooNoBarInstance.make())
        metricDictionary.add(regFoosInstance.make())
        metricDictionary.add(foos.make())
        metricDictionary.add(dayAvgFoos.make())

        metricDictionary.add(foo.make())
        metricDictionary.add(wiz.make())
        metricDictionary.add(waz.make())
        metricDictionary.add(unregFoos.make())
        metricDictionary.add(viz.make())

        LogicalMetric foosMetric = metricDictionary.get(FOOS.asName())
        LogicalMetric ratioMetric = new LogicalMetric(foosMetric.templateDruidQuery, foosMetric.calculation, "ratioMetric", "ratioMetric Long Name", "Ratios", "Dummy metric Ratio Metric description", {true})
        metricDictionary.add(ratioMetric)

        LogicalMetricColumn lmc = new LogicalMetricColumn(foosMetric);

        columns.add(lmc)

        PhysicalTable physicalTable = new ConcretePhysicalTable(
                "NETWORK",
                DAY.buildZonedTimeGrain(UTC),
                columns,
                ["property": "property", "country": "country"],
                Mock(DataSourceMetadataService)
        )

        TableGroup tableGroup = new TableGroup([physicalTable] as LinkedHashSet, metrics)

        table = new LogicalTable("NETWORK", DAY, tableGroup, metricDictionary)

        JSONArray metricJsonObjArray = new JSONArray("[{\"filter\":{\"AND\":\"country|id-in[US,IN],property|id-in[114,125]\"},\"name\":\"foo\"},{\"filter\":{},\"name\":\"pageviews\"}]")
        JSONObject jsonobject = metricJsonObjArray.getJSONObject(0)
        filterObj = jsonobject.getJSONObject("filter")

        fooPostAggregation = foos.make().templateDruidQuery.getPostAggregations().first()

        dayAvgFoosTdq = dayAvgFoos.make().templateDruidQuery;

        fooNoBarAggregation = fooNoBarInstance.make().templateDruidQuery.aggregations.first()
        Aggregation regFoosAggregation = regFoosInstance.make().templateDruidQuery.aggregations.first()

        fooNoBarFilteredAggregationSet = FieldConverterSupplier.metricsFilterSetBuilder.getFilteredAggregation(filterObj, fooNoBarAggregation, dimensionDictionary, table, new DataApiRequest())
        fooNoBarPostAggregationInterim = ThetaSketchSetOperationHelper.makePostAggFromAgg(SketchSetOperationPostAggFunction.INTERSECT, "fooNoBar", new ArrayList<>(fooNoBarFilteredAggregationSet))

        fooRegFoosFilteredAggregationSet = FieldConverterSupplier.metricsFilterSetBuilder.getFilteredAggregation(filterObj, regFoosAggregation, dimensionDictionary, table, new DataApiRequest())
        fooRegFoosPostAggregationInterim = ThetaSketchSetOperationHelper.makePostAggFromAgg(SketchSetOperationPostAggFunction.INTERSECT, "regFoos", new ArrayList<>(fooRegFoosFilteredAggregationSet))

        interimPostAggDictionary = [:]
        interimPostAggDictionary.put(fooNoBarAggregation.getName(), fooNoBarFilteredAggregationSet as List)
        interimPostAggDictionary.put(regFoosAggregation.getName(), fooRegFoosFilteredAggregationSet as List)

        return this
    }
}
