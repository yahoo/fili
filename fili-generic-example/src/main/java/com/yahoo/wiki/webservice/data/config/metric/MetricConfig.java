package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.metric.MetricInstance;
import com.yahoo.bard.webservice.data.config.metric.makers.AggregationAverageMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.ArithmeticMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.CountMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleMaxMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleMinMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleSumMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.LongMaxMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.LongMinMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.LongSumMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.RowNumMaker;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;
import com.yahoo.bard.webservice.util.EnumUtils;
import com.yahoo.wiki.webservice.data.config.metric.DruidMetricName;
import com.yahoo.wiki.webservice.data.config.metric.FiliApiMetricName;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Created by hinterlong on 7/5/17.
 */
public class MetricConfig {
    private final String apiMetricName;
    private final List<String> dependentMetricNames;
    private final List<TimeGrain> validTimeGrains;
    private final List<String> params;
    private final MetricMakerType makerType;

    public MetricConfig(
            String apiMetricName,
            List<String> dependentMetricNames,
            List<TimeGrain> validTimeGrains,
            List<String> params,
            String type
    ) {
        this(apiMetricName, dependentMetricNames, validTimeGrains, params, MetricMakerType.fromType(type));
    }

    public MetricConfig(
            String apiMetricName,
            List<String> dependentMetricNames,
            List<TimeGrain> validTimeGrains,
            List<String> params,
            MetricMakerType makerType
    ) {
        this.apiMetricName = apiMetricName;
        this.dependentMetricNames = dependentMetricNames;
        this.validTimeGrains = validTimeGrains;
        this.params = params;
        this.makerType = makerType;
    }

    public String getApiMetricName() {
        return apiMetricName;
    }

    public ApiMetricName getFiliApiMetricName() {
        return new FiliApiMetricName(apiMetricName, validTimeGrains);
    }

    public FieldName getDruidMetricName() {
        if(isDruidMetric()) {
            return new DruidMetricName(dependentMetricNames.get(0));
        } else {
            return new DruidMetricName(apiMetricName);
        }
    }

    public MetricInstance getMetricInstance(MetricDictionary metricDictionary) {
        String[] fieldNames = dependentMetricNames.toArray(new String[dependentMetricNames.size()]);
        return new MetricInstance(
                apiMetricName,
                buildMetricMaker(metricDictionary),
                fieldNames
        );
    }

    private MetricMaker buildMetricMaker(MetricDictionary metricDictionary) {
        return makerType.getMetricMakerClass(metricDictionary, params);
    }

    public boolean isDruidMetric() {
        return dependentMetricNames.size() == 1 && dependentMetricNames.get(0).equals(apiMetricName);
    }

    public enum MetricMakerType {
        LONG_MIN((metricDictionary, params) -> new LongMinMaker(metricDictionary)),
        LONG_MAX((metricDictionary, params) -> new LongMaxMaker(metricDictionary)),
        LONG_SUM((metricDictionary, params) -> new LongSumMaker(metricDictionary)),
        DOUBLE_MIN((metricDictionary, params) -> new DoubleMinMaker(metricDictionary)),
        DOUBLE_MAX((metricDictionary, params) -> new DoubleMaxMaker(metricDictionary)),
        DOUBLE_SUM((metricDictionary, params) -> new DoubleSumMaker(metricDictionary)),
        ARITHMETIC((metricDictionary, params) -> new ArithmeticMaker(
                metricDictionary,
                ArithmeticPostAggregation.ArithmeticPostAggregationFunction.fromDruidName(params.get(0))
        )),
        AGGREGATION_AVERAGE((metricDictionary, params) -> new AggregationAverageMaker(
                metricDictionary,
                DefaultTimeGrain.valueOf(params.get(0))
        )),
        COUNT((metricDictionary, params) -> new CountMaker(metricDictionary)),
        ROW_NUM((metricDictionary, params) -> new RowNumMaker(metricDictionary));

        private final String type;
        private final BiFunction<MetricDictionary, List<String>, MetricMaker> metricMaker;

        MetricMakerType(BiFunction<MetricDictionary, List<String>, MetricMaker> metricMaker) {
            type = EnumUtils.camelCase(name());
            this.metricMaker = metricMaker;
        }

        public static MetricMakerType fromType(String type) {
            for (MetricMakerType metricMakerType : values()) {
                if (metricMakerType.type.equals(type)) {
                    return metricMakerType;
                }
            }
            return null;
        }

        public MetricMaker getMetricMakerClass(MetricDictionary metricDictionary, List<String> params) {
            return metricMaker.apply(metricDictionary, params);
        }
    }
}
