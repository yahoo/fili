// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

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
    public DimensionDictionary dimensionDict
    public MetricDictionary metricDict
    public LogicalTable table
    public JSONObject filterObj
    public PostAggregation bcookiePostAggregation;
    public Filter filter;
    public Set<FilteredAggregation> bcookieNoYuidFilteredAggregationSet
    public Set<FilteredAggregation> bcookieRegBcookiesFilteredAggregationSet
    public ThetaSketchSetOperationPostAggregation bcookieNoYuidPostAggregationInterim
    public Map<String, List<FilteredAggregation>> interimPostAggDictionary
    public ThetaSketchSetOperationPostAggregation bcookieRegBcookiesPostAggregationInterim
    public Aggregation bcookieNoYuidAggregation
    public TemplateDruidQuery dayAvgBcookiesTdq
    public Dimension propertyDim
    public Dimension countryDim

    ThetaSketchIntersectionReportingResources init() {
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        //Initializing the Sketch field converters
        FieldConverterSupplier.sketchConverter = new ThetaSketchFieldConverter();
        FieldConverterSupplier.metricsFilterSetBuilder = new FilteredThetaSketchMetricsHelper();

        propertyDim = new KeyValueStoreDimension(
                "property",
                "property",
                null,
                [BardDimensionField.ID] as LinkedHashSet,
                MapStoreManager.getInstance("property"),
                ScanSearchProviderManager.getInstance("property")
        )

        countryDim = new KeyValueStoreDimension(
                "country",
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

        PhysicalTable physicalTable = new PhysicalTable("NETWORK", DAY)

        //added dimensions to the physical table
        [propertyDim, countryDim].each {
            physicalTable.addColumn(DimensionColumn.addNewDimensionColumn(physicalTable, it))
        }

        Set<ApiMetricName> metrics = [buildMockName("bcookies"), buildMockName("bcookieNoYuid"), buildMockName("regBcookies"), buildMockName("pageViews"), buildMockName("yuid"), buildMockName("sid"), buildMockName("regUser"), buildMockName("uniqueIdentifier"), buildMockName("unregBcookies"), buildMockName("ratioMetric")]
        //added metrics to the physical table
        metrics.each {
            physicalTable.addColumn(MetricColumn.addNewMetricColumn(physicalTable, it.apiName))
        }

        physicalTable.commit()

        TableGroup tableGroup = new TableGroup([physicalTable] as LinkedHashSet, metrics)
        table = new LogicalTable("NETWORK", DAY, tableGroup)

        for (Dimension dim : tableGroup.getDimensions()) {
            DimensionColumn.addNewDimensionColumn(table, dim)
        }

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
        MetricInstance bcookieNoYuidInstance = new MetricInstance("bcookieNoYuid", ThetaSketchMaker, "bcookieNoYuid")
        MetricInstance regBcookiesInstance = new MetricInstance("regBcookies", ThetaSketchMaker, "regBcookies")
        MetricInstance bcookies = new MetricInstance("bcookies", setUnionMaker, "bcookieNoYuid", "regBcookies")
        MetricInstance dayAvgBcookies = new MetricInstance("dayAvgBcookies", simpleDailyAverageMaker, "bcookies")

        MetricInstance yuid = new MetricInstance("yuid", ThetaSketchMaker, "yuid")
        MetricInstance sid = new MetricInstance("sid", ThetaSketchMaker, "sid")
        MetricInstance regUser = new MetricInstance("regUser", sumMaker, "yuid", "sid")
        MetricInstance unregBcookies = new MetricInstance("unregBcookies", setDifferenceMaker, "bcookieNoYuid", "regBcookies")
        MetricInstance uniqueIdentifier = new MetricInstance("uniqueIdentifier", sumMaker, "regUser", "unregBcookies")

        metricDict.add(pageViews.make())
        metricDict.add(bcookieNoYuidInstance.make())
        metricDict.add(regBcookiesInstance.make())
        metricDict.add(bcookies.make())
        metricDict.add(dayAvgBcookies.make())

        metricDict.add(yuid.make())
        metricDict.add(sid.make())
        metricDict.add(regUser.make())
        metricDict.add(unregBcookies.make())
        metricDict.add(uniqueIdentifier.make())

        LogicalMetric ratioMetric = new LogicalMetric(metricDict.get("bcookies").templateDruidQuery, metricDict.get("bcookies").calculation, "ratioMetric", "ratioMetric Long Name", "Ratios", "Dummy metric Ratio Metric description")
        metricDict.add(ratioMetric)

        LogicalMetricColumn lmc = new LogicalMetricColumn("bcookies", bcookies.make());
        table.addColumn(lmc)

        JSONArray metricJsonObjArray = new JSONArray("[{\"filter\":{\"AND\":\"country|id-in[US,IN],property|id-in[114,125]\"},\"name\":\"bcookie\"},{\"filter\":{},\"name\":\"pageviews\"}]")
        JSONObject jsonobject = metricJsonObjArray.getJSONObject(0)
        filterObj = jsonobject.getJSONObject("filter")

        bcookiePostAggregation = bcookies.make().templateDruidQuery.getPostAggregations().first()

        dayAvgBcookiesTdq = dayAvgBcookies.make().templateDruidQuery;

        bcookieNoYuidAggregation = bcookieNoYuidInstance.make().templateDruidQuery.aggregations.first()
        Aggregation regBcookiesAggregation = regBcookiesInstance.make().templateDruidQuery.aggregations.first()

        bcookieNoYuidFilteredAggregationSet = FieldConverterSupplier.metricsFilterSetBuilder.getFilteredAggregation(filterObj, bcookieNoYuidAggregation, dimensionDict, table, new DataApiRequest())
        bcookieNoYuidPostAggregationInterim = ThetaSketchSetOperationHelper.makePostAggFromAgg(SketchSetOperationPostAggFunction.INTERSECT, "bcookieNoYuid", new ArrayList<>(bcookieNoYuidFilteredAggregationSet))

        bcookieRegBcookiesFilteredAggregationSet = FieldConverterSupplier.metricsFilterSetBuilder.getFilteredAggregation(filterObj, regBcookiesAggregation, dimensionDict, table, new DataApiRequest())
        bcookieRegBcookiesPostAggregationInterim = ThetaSketchSetOperationHelper.makePostAggFromAgg(SketchSetOperationPostAggFunction.INTERSECT, "regBcookies", new ArrayList<>(bcookieRegBcookiesFilteredAggregationSet))

        interimPostAggDictionary = [:]
        interimPostAggDictionary.put(bcookieNoYuidAggregation.getName(), bcookieNoYuidFilteredAggregationSet as List)
        interimPostAggDictionary.put(regBcookiesAggregation.getName(), bcookieRegBcookiesFilteredAggregationSet as List)

        return this
    }

    ApiMetricName buildMockName(String name) {
        Stub(ApiMetricName) {
            getApiName() >> name
            isValidFor(_) >> true
        }
    }
}
