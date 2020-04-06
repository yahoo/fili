// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol.protocols;

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK;

import com.yahoo.bard.webservice.data.config.metric.makers.AggregationAverageMaker;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.protocol.DefaultSystemMetricProtocols;
import com.yahoo.bard.webservice.data.metric.protocol.GeneratedMetricInfo;
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
 * Transformer for applying TimeAverage transformation at runtime using the Protocol Metric API. The details of the
 * time average algorithm are specified in the {@link AggregationAverageMaker} class.
 */
public class TimeAverageMetricTransformer implements MetricTransformer {

    private static final MetricDictionary EMPTY_METRIC_DICTIONARY = new MetricDictionary();

    private Map<String, TimeAverageMetricMakerConfig> makerConfigMap;

    private Map<String, AggregationAverageMaker> metricMakerMap;
    private final Supplier<ProtocolSupport> protocolSupportSupplier;

    public static Collection<String> acceptedValues() {
        return TimeAverageMetricMakerConfig.timeMakerConfigs.keySet();
    }

    private final MetricTransformer delegate;

    /**
     * Constructor.
     *
     * Left protected to allow subclasses to add custom time intervals for reaggregation.
     *
     * @param protocolSupportSupplier  A source for the default protocol support used by the makers.
     * @param  makerConfigMap  A collection of maker configurations to use when making time reaggregations.
     * @param  delegate  A metric transformer to send to if no matching core value is found in this one.ß
     */
    public TimeAverageMetricTransformer(
            Supplier<ProtocolSupport> protocolSupportSupplier,
            Map<String, TimeAverageMetricMakerConfig> makerConfigMap,
            MetricTransformer delegate
    ) {
        this.protocolSupportSupplier = protocolSupportSupplier;
        this.makerConfigMap = makerConfigMap;
        metricMakerMap = new HashMap<>();
        this.delegate = delegate;
    }

    /**
     * Constructor.
     *
     * Private to implement singleton pattern for normal usage.
     */
    public TimeAverageMetricTransformer() {
        this(
                DefaultSystemMetricProtocols::getStandardProtocolSupport,
                TimeAverageMetricMakerConfig.timeMakerConfigs,
                MetricTransformer.ERROR_THROWING_TRANSFORM
        );
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
    public LogicalMetric apply(
            GeneratedMetricInfo resultMetadata,
            LogicalMetric logicalMetric,
            Protocol protocol,
            Map<String, String> parameterValues
    )
            throws UnknownProtocolValueException {

        String parameterValue = parameterValues.get(protocol.getCoreParameterName());

        if (!makerConfigMap.containsKey(parameterValue)) {
            return delegate.apply(resultMetadata, logicalMetric, protocol, parameterValues);
        }
        TimeAverageMetricMakerConfig metricMakerConfig = makerConfigMap.get(parameterValue);

        AggregationAverageMaker maker = metricMakerMap.computeIfAbsent(
                parameterValue,
                key -> buildMaker(metricMakerConfig)
        );

        return maker.makeInnerWithResolvedDependencies(resultMetadata, Collections.singletonList(logicalMetric));
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
        private final ZonelessTimeGrain grain;

        public static final TimeAverageMetricMakerConfig DAY_AVERAGE = new TimeAverageMetricMakerConfig(
                "dayAvg",
                DAY
        );

        public static final TimeAverageMetricMakerConfig WEEK_AVERAGE = new TimeAverageMetricMakerConfig(
                "weekAvg",
                WEEK
        );

        public static final TimeAverageMetricMakerConfig MONTH_AVERAGE = new TimeAverageMetricMakerConfig(
                "monthAvg",
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
         * @return The grain of the metric maker for this config.
         */
        public ZonelessTimeGrain getGrain() {
            return grain;
        }

        /**
         * Build a configuration bean that helps configure grain specific reaggregation.
         *
         * @param parameterValue  The value of the parameter that uses this config.
         * @param granularity  The granularity of reaggregation.
         */
        public TimeAverageMetricMakerConfig(
                String parameterValue,
                ZonelessTimeGrain granularity
        ) {
            this.parameterValue = parameterValue;
            this.grain = granularity;
        }
    }
}
