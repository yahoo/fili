// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric;

import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_AREA;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_DAY_AVG_LIMBS;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_DAY_AVG_OTHER_USERS;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_DAY_AVG_USERS;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_DEPTH;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_HEIGHT;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_LIMBS;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_OTHER_USERS;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_ROW_NUM;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_SCOPED_WIDTH;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_USERS;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_VOLUME;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_WIDTH;
import static com.yahoo.bard.webservice.data.config.names.TestDruidMetricName.DEPTH;
import static com.yahoo.bard.webservice.data.config.names.TestDruidMetricName.HEIGHT;
import static com.yahoo.bard.webservice.data.config.names.TestDruidMetricName.LIMBS;
import static com.yahoo.bard.webservice.data.config.names.TestDruidMetricName.USERS;
import static com.yahoo.bard.webservice.data.config.names.TestDruidMetricName.WIDTH;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY;

import com.yahoo.bard.webservice.data.config.metric.makers.AggregationAverageMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.ArithmeticMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.LongSumMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.RowNumMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchMaker;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Load the Test-specific metrics.
 */
public class TestMetricLoader implements MetricLoader {

    public static final int BYTES_PER_KILOBYTE = 1024;
    public static final int DEFAULT_KILOBYTES_PER_SKETCH = 16;
    public static final int DEFAULT_SKETCH_SIZE_IN_BYTES = DEFAULT_KILOBYTES_PER_SKETCH * BYTES_PER_KILOBYTE;

    public int sketchSize;

    // Aggregator Makers
    public LongSumMaker longSumMaker;
    public ThetaSketchMaker sketchMaker;

    // Post Aggregator Makers
    public ArithmeticMaker productMaker;
    public AggregationAverageMaker simpleDailyAverageMaker;
    public RowNumMaker rowNumMaker;

    /**
     * Constructs a TestMetricLoader using the default sketch size.
     */
    public TestMetricLoader() {
        this(DEFAULT_SKETCH_SIZE_IN_BYTES);
    }

    /**
     * Constructs a TestMetricLoader using the given sketch size.
     *
     * @param sketchSize  Sketch size, in number of bytes, to use for sketch operations
     */
    public TestMetricLoader(int sketchSize) {
        this.sketchSize = sketchSize;
    }

    /**
     * (Re)Initialize the metric makers with the given metric dictionary.
     *
     * @param metricDictionary  Metric dictionary to use for generating the metric makers.
     */
    protected void buildMetricMakers(MetricDictionary metricDictionary) {
        // Create the various metric makers
        longSumMaker = new LongSumMaker(metricDictionary);
        sketchMaker = new ThetaSketchMaker(metricDictionary, sketchSize);
        productMaker = new ArithmeticMaker(metricDictionary, ArithmeticPostAggregationFunction.MULTIPLY);
        simpleDailyAverageMaker = new AggregationAverageMaker(metricDictionary, DAY);
        rowNumMaker = new RowNumMaker(metricDictionary);
    }

    @Override
    public void loadMetricDictionary(MetricDictionary metricDictionary, DimensionDictionary dimensionDictionary) {
        buildMetricMakers(metricDictionary);

        // Metrics that directly aggregate druid fields
        List<MetricInstance> metrics = Arrays.asList(
                new MetricInstance((ApiMetricName) A_HEIGHT, longSumMaker, HEIGHT),
                new MetricInstance(A_WIDTH, longSumMaker, WIDTH),
                new MetricInstance(A_DEPTH, longSumMaker, DEPTH),
                new MetricInstance(A_LIMBS, longSumMaker, LIMBS),
                new MetricInstance(A_USERS, sketchMaker, USERS),
                new MetricInstance(A_OTHER_USERS, sketchMaker, USERS),
                new MetricInstance(A_ROW_NUM, rowNumMaker, new FieldName[] {}),
                new MetricInstance(A_AREA, productMaker, A_HEIGHT, A_WIDTH),
                new MetricInstance(A_VOLUME, productMaker, A_HEIGHT, A_WIDTH, A_DEPTH),
                new MetricInstance(A_DAY_AVG_USERS, simpleDailyAverageMaker, A_USERS),
                new MetricInstance(A_DAY_AVG_OTHER_USERS, simpleDailyAverageMaker, A_OTHER_USERS),
                new MetricInstance(A_DAY_AVG_LIMBS, simpleDailyAverageMaker, A_LIMBS)
        );

        metrics.stream().map(MetricInstance::make).forEach(metricDictionary::add);
        metricDictionary.getScope("shapes").add(
                new MetricInstance(new LogicalMetricInfo(A_SCOPED_WIDTH.asName()), longSumMaker, WIDTH.asName()).make()
        );
        //Allows us to add some non-numeric LogicalMetrics without having to write a Maker for them. Makers should
        //be written if using complex metrics in production code.
        NonNumericMetrics.getLogicalMetrics().forEach(metricDictionary::add);
    }
}
