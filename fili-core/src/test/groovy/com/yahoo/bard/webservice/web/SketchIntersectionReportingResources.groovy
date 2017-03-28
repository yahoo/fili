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
import com.yahoo.bard.webservice.table.ConcretePhysicalTable
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
    public DimensionDictionary dimensionDictionary
    public MetricDictionary metricDictionary
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


        dimensionDictionary = new DimensionDictionary()
        dimensionDictionary.addAll([propertyDim, countryDim])


        //added dimensions to the physical table
        Set columns = [propertyDim, countryDim].collect() { new DimensionColumn(it)}

        // regFoos deliberately omitted
        Set<ApiMetricName> metrics = [FOOS, FOO_NO_BAR, PAGE_VIEWS, FOO, WIZ, WAZ, VIZ, UNREG_FOOS, RATIO_METRIC]
        //added metrics to the physical table
        columns.addAll( metrics.collect() { new MetricColumn(it.apiName)})

        metricDictionary = new MetricDictionary()

        SketchSetOperationMaker setUnionMaker = new SketchSetOperationMaker(
                metricDictionary,
                SketchSetOperationPostAggFunction.UNION
        )
        SketchCountMaker sketchCountMaker = new SketchCountMaker(new MetricDictionary(), 16384)
        ArithmeticMaker sumMaker = new ArithmeticMaker(metricDictionary, ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS);
        SketchSetOperationMaker setDifferenceMaker = new SketchSetOperationMaker(metricDictionary, SketchSetOperationPostAggFunction.NOT);
        AggregationAverageMaker simpleDailyAverageMaker = new AggregationAverageMaker(metricDictionary, DAY)


        MetricInstance pageViews = new MetricInstance(PAGE_VIEWS, new LongSumMaker(metricDictionary), PAGE_VIEWS)
        MetricInstance fooNoBarInstance = new MetricInstance(FOO_NO_BAR, sketchCountMaker, FOO_NO_BAR)
        MetricInstance regFoosInstance = new MetricInstance(REG_FOOS, sketchCountMaker, REG_FOOS)
        MetricInstance foos = new MetricInstance(FOOS, setUnionMaker, FOO_NO_BAR, REG_FOOS)
        MetricInstance dayAvgFoos = new MetricInstance(DAY_AVG_FOOS, simpleDailyAverageMaker, FOOS)

        MetricInstance foo = new MetricInstance(FOO, sketchCountMaker, FOO)
        MetricInstance wiz = new MetricInstance(WIZ, sketchCountMaker, WIZ)
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
                [:],
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
        fooNoBarPostAggregationInterim = SketchSetOperationHelper.makePostAggFromAgg(SketchSetOperationPostAggFunction.INTERSECT, "fooNoBar", new ArrayList<>(fooNoBarFilteredAggregationSet))

        fooRegFoosFilteredAggregationSet = FieldConverterSupplier.metricsFilterSetBuilder.getFilteredAggregation(filterObj, regFoosAggregation, dimensionDictionary, table, new DataApiRequest())
        fooRegFoosPostAggregationInterim = SketchSetOperationHelper.makePostAggFromAgg(SketchSetOperationPostAggFunction.INTERSECT, "regFoos", new ArrayList<>(fooRegFoosFilteredAggregationSet))

        interimPostAggDictionary = [:]
        interimPostAggDictionary.put(fooNoBarAggregation.getName(), fooNoBarFilteredAggregationSet as List)
        interimPostAggDictionary.put(regFoosAggregation.getName(), fooRegFoosFilteredAggregationSet as List)

        return this
    }
}
