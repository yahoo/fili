package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.config.metric.MetricInstance;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleMaxMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleMinMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleSumMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.LongMaxMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.LongMinMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.LongSumMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.util.EnumUtils;
import com.yahoo.wiki.webservice.data.config.metric.DruidMetricName;
import com.yahoo.wiki.webservice.data.config.metric.FiliApiMetricName;

import java.util.List;
import java.util.function.Function;

/**
 * Created by hinterlong on 7/5/17.
 */
public class MetricConfig {
    private final String apiMetricName;
    private final String physicalMetricName;
    private final List<TimeGrain> validTimeGrains;
    private final MetricMakerType makerType;

    public MetricConfig(
            String apiMetricName,
            String physicalMetricName,
            List<TimeGrain> validTimeGrains,
            String type
    ) {
        this(apiMetricName, physicalMetricName, validTimeGrains, MetricMakerType.fromType(type));
    }

    public MetricConfig(
            final String apiMetricName,
            final String physicalMetricName,
            final List<TimeGrain> validTimeGrains,
            final MetricMakerType makerType
    ) {
        this.apiMetricName = apiMetricName;
        this.physicalMetricName = physicalMetricName;
        this.validTimeGrains = validTimeGrains;
        this.makerType = makerType;
    }

    public MetricInstance getMetricInstance(MetricMaker metricMaker) {
        ApiMetricName filiApiMetricName = new FiliApiMetricName(apiMetricName, validTimeGrains);
        FieldName fieldName = new DruidMetricName(physicalMetricName);
        return new MetricInstance(filiApiMetricName, metricMaker, fieldName);
    }

    public String getApiMetricName() {
        return apiMetricName;
    }

    public ApiMetricName getFiliApiMetricName() {
        return new FiliApiMetricName(apiMetricName, validTimeGrains);
    }

    public String getPhysicalMetricName() {
        return physicalMetricName;
    }

    public FieldName getDruidMetricName() {
        return new DruidMetricName(physicalMetricName);
    }

    public MetricInstance getMetricInstance(MetricDictionary metricDictionary) {
        return new MetricInstance(apiMetricName, buildMetricMaker(metricDictionary), physicalMetricName);
    }

    private MetricMaker buildMetricMaker(MetricDictionary metricDictionary) {
        return makerType.getMetricMakerClass(metricDictionary);
    }

    public enum MetricMakerType {
        LONG_MIN(LongMinMaker::new),
        LONG_MAX(LongMaxMaker::new),
        LONG_SUM(LongSumMaker::new),
        DOUBLE_MIN(DoubleMinMaker::new),
        DOUBLE_MAX(DoubleMaxMaker::new),
        DOUBLE_SUM(DoubleSumMaker::new);

        private final String type;
        private final Function<MetricDictionary, MetricMaker> metricMaker;

        MetricMakerType(Function<MetricDictionary, MetricMaker> metricMaker) {
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

        public MetricMaker getMetricMakerClass(MetricDictionary metricDictionary) {
            return metricMaker.apply(metricDictionary);
        }
    }
}
