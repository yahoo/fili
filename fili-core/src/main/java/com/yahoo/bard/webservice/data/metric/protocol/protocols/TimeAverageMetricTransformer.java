// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol.protocols;

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK;

import com.yahoo.bard.webservice.data.config.metric.makers.AggregationAverageMaker;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.protocol.DefaultSystemMetricProtocols;
import com.yahoo.bard.webservice.data.metric.protocol.MetricTransformer;
import com.yahoo.bard.webservice.data.metric.protocol.Protocol;
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolSupport;
import com.yahoo.bard.webservice.data.metric.protocol.UnknownProtocolValueException;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * An interface for transforming metrics into other metrics.
 */
public class TimeAverageMetricTransformer implements MetricTransformer {

    private static final MetricDictionary EMPTY_METRIC_DICTIONARY = new MetricDictionary();
    public final static TimeAverageMetricTransformer INSTANCE = new TimeAverageMetricTransformer();

    private static final String NAME_FORMAT = "%s%s";
    private static final String LONG_NAME_FORMAT = "%s (%s)";
    private static final String DESCRIPTION_FORMAT = "The %s of %s.";

    private Map<String, TimeAverageMetricMakerConfig> makerConfigMap;

    private Map<String, AggregationAverageMaker> metricMakerMap;
    private final Supplier<ProtocolSupport> protocolSupportSupplier;

    public static Collection<String> acceptedValues() {
        return TimeAverageMetricMakerConfig.timeMakerConfigs.keySet();
    }

    /**
     * Constructor.
     *
     * Left protected to allow subclasses to add custom time intervals for reaggregation.
     *
     * @param protocolSupportSupplier  A source for the default protocol support used by the makers.
     * @param  makerConfigMap  A collection of maker configurations to use when making time reaggregations.
     */
    protected TimeAverageMetricTransformer(
            Supplier<ProtocolSupport> protocolSupportSupplier,
            Map<String, TimeAverageMetricMakerConfig> makerConfigMap
    ) {
        this.protocolSupportSupplier = protocolSupportSupplier;
        this.makerConfigMap = makerConfigMap;
        metricMakerMap = new HashMap<>();
    }

    /**
     * Constructor.
     *
     * Private to implement singleton pattern for normal usage.
     */
    private TimeAverageMetricTransformer() {
        this(DefaultSystemMetricProtocols::getStandardProtocolSupport, TimeAverageMetricMakerConfig.timeMakerConfigs);
    }

    /**
     * Build a MetricMaker for this configuration.
     *
     * @param makerConfig  The configuration for the metric maker.
     *
     * @return  A metric maker for this configuration.
     */
    private AggregationAverageMaker buildMaker(TimeAverageMetricMakerConfig makerConfig) {
        return new AggregationAverageMaker(
                EMPTY_METRIC_DICTIONARY,
                makerConfig.getGrain(),
                protocolSupportSupplier.get()
        );
    }
    @Override
    public LogicalMetric apply(LogicalMetric logicalMetric, Protocol protocol, Map<String, String> parameterValues)
            throws UnknownProtocolValueException {

        String parameterValue = parameterValues.get(protocol.getCoreParameterName());

        if (!makerConfigMap.containsKey(parameterValue)) {
            throw new UnknownProtocolValueException(protocol, parameterValues);
        }
        TimeAverageMetricMakerConfig metricMakerConfig = makerConfigMap.get(parameterValue);

        AggregationAverageMaker maker = metricMakerMap.computeIfAbsent(
                parameterValue,
                key -> buildMaker(metricMakerConfig)
        );

        LogicalMetricInfo info = makeNewLogicalMetricInfo(logicalMetric.getLogicalMetricInfo(), metricMakerConfig);
        return maker.makeInnerWithResolvedDependencies(info, Collections.singletonList(logicalMetric));
    }

    /**
     * Build the new identity metadata for the transformed metric.
     *
     * @param info  The identity metadata from the existing metric
     * @param config The descriptors for the maker type being built
     *
     * @return  A metric info for a time-ly logical metric.
     */
    protected LogicalMetricInfo makeNewLogicalMetricInfo(LogicalMetricInfo info, TimeAverageMetricMakerConfig config) {
        String name = String.format(NAME_FORMAT, config.getNamePrefix(), info.getName());
        String longName = String.format(LONG_NAME_FORMAT, info.getLongName(), config.getLongNameSuffix());
        String description = String.format(DESCRIPTION_FORMAT, config.getLongNameSuffix(), info.getDescription());
        return new LogicalMetricInfo(name, longName, info.getCategory(), description, info.getType());
    }

    /**
     * Bean describing the information needed to support building a time aggregation.
     *
     * If an implementing  system requires time aggregation of additional custom grains, it can build instances and add
     * them to the timeMakerConfigs static map.
     */
    public static class TimeAverageMetricMakerConfig {

        public static Map<String, TimeAverageMetricMakerConfig> timeMakerConfigs = new LinkedHashMap<>();

        private final String parameterValue;
        private final String namePrefix;
        private final String longNameSuffix;
        private final ZonelessTimeGrain grain;

        public static final TimeAverageMetricMakerConfig DAY_AVERAGE = new TimeAverageMetricMakerConfig(
                "dayAvg",
                "dayAvg",
                "Daily Average",
                DAY
        );

        public static final TimeAverageMetricMakerConfig WEEK_AVERAGE = new TimeAverageMetricMakerConfig(
                "weekAvg",
                "weekAvg",
                "Weekly Average",
                WEEK
        );

        public static final TimeAverageMetricMakerConfig MONTH_AVERAGE = new TimeAverageMetricMakerConfig(
                "monthAvg",
                "monthAvg",
                "Monthly Average",
                MONTH
        );

        static {
            timeMakerConfigs.put(DAY_AVERAGE.getParameterValue(), DAY_AVERAGE);
            timeMakerConfigs.put(WEEK_AVERAGE.getParameterValue(), WEEK_AVERAGE);
            timeMakerConfigs.put(MONTH_AVERAGE.getParameterValue(), MONTH_AVERAGE);
        }

        /**
         * Getter.
         *
         * @return  The parameter value that triggers this config.
         */
        public String getParameterValue() {
            return parameterValue;
        }

        /**
         * Getter.
         *
         * @return The name added to the logical metric info name.
         */
        public String getNamePrefix() {
            return namePrefix;
        }

        /**
         * Getter.
         *
         * @return The longName added to the logical metric info long name and description.
         */
        public String getLongNameSuffix() {
            return longNameSuffix;
        }

        /**
         * Getter.
         *
         * @return The grain of the metric maker for this config.
         */
        public ZonelessTimeGrain getGrain() {
            return grain;
        }

        /**
         * Build a configuration bean that helps configure grain specific reaggregation.
         *
         * @param parameterValue  The value of the parameter that uses this config.
         * @param namePrefix   The prefix added to the metric apiName.
         * @param longNameSuffix  The suffix added to longNames.
         * @param granularity  The granularity of reaggregation.
         */
        TimeAverageMetricMakerConfig(
                String parameterValue,
                String namePrefix,
                String longNameSuffix,
                ZonelessTimeGrain granularity
        ) {
            this.parameterValue = parameterValue;
            this.namePrefix = namePrefix;
            this.longNameSuffix = longNameSuffix;
            this.grain = granularity;
        }
    }
}
