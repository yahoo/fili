package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.metric.makers.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.*;

/**
 * MetricMaker dictionary.
 */
@Singleton
public class MetricMakerDictionary {

    private static final Logger LOG = LoggerFactory.getLogger(MetricMakerDictionary.class);

    /**
     * Maps for Metric Maker names to Metric Makers.
     */
    private final LinkedHashMap<String, Class<? extends MetricMaker>> nameToMetricMaker;

    /**
     * Constructor
     */
    public MetricMakerDictionary() {
        nameToMetricMaker = new LinkedHashMap<>();
    }

    public MetricMakerDictionary(boolean useDefault) {
        nameToMetricMaker = new LinkedHashMap<>();
        if (!useDefault) return;

        add(AggregationAverageMaker.class);
        add(ArithmeticMaker.class);
        add(CardinalityMaker.class);
        add(ConstantMaker.class);
        add(CountMaker.class);
        add(DoubleMaxMaker.class);
        add(DoubleMinMaker.class);
        add(DoubleSumMaker.class);
        add(FilteredAggregationMaker.class);
        add(LongMaxMaker.class);
        add(LongMinMaker.class);
        add(LongSumMaker.class);
        add(MaxMaker.class);
        add(MinMaker.class);
        add(RawAggregationMetricMaker.class);
        add(RowNumMaker.class);
        add(SketchCountMaker.class);
        add(SketchSetOperationMaker.class);
        add(ThetaSketchMaker.class);
        add(ThetaSketchSetOperationMaker.class);

    }

    public MetricMakerDictionary(Set<Class<? extends MetricMaker>> metricMakers) {
        this();
        addAll(metricMakers);
    }

    /**
     * Find a Metric Maker given a Metric Maker Name.
     *
     * @param metricMakerName  Name to search
     *
     * @return the first Metric Maker found (if exists)
     */
    public Class<? extends MetricMaker> findByName(String metricMakerName) {
        return nameToMetricMaker.get(metricMakerName);
    }

    /**
     * Get all Metric Makers available in MetricMaker dictionary.
     *
     * @return a set of Metric Makers
     */
    public Set<Class<? extends MetricMaker>> findAll() {
        return Collections.unmodifiableSet(new HashSet<>(nameToMetricMaker.values()));
    }

    /**
     * Adds the specified element to the dictionary if it is not already present.
     *
     * @param metricMaker element to add to dictionary
     *
     * @return <tt>true</tt> if the dictionary did not already contain the specified Metric Maker
     * @see Set#add(Object)
     */
    public boolean add(Class<? extends MetricMaker> metricMaker) {
        String makerName = metricMaker.getSimpleName().replace("Maker","").toLowerCase();
        if (nameToMetricMaker.containsKey(makerName)) {
            return false;
        }
        Class<? extends MetricMaker> metricMakers = nameToMetricMaker.put(makerName, metricMaker);
        if (metricMakers != null) {
            // should never happen unless multiple loaders are running in race-condition
            ConcurrentModificationException e = new ConcurrentModificationException();
            LOG.error("Multiple loaders updating MetricMakerDictionary", e);
            throw e;
        }
        return true;
    }

    /**
     * Adds all of the metricMakers in the specified collection to the dictionary.
     *
     * @param metricMakers collection of Metric Makers to add
     *
     * @return <tt>true</tt> if the dictionary changed as a result of the call
     * @see Set#addAll(Collection)
     */
    public boolean addAll(Collection<Class<? extends MetricMaker>> metricMakers) {
        boolean flag = false;
        for (Class<? extends MetricMaker> metricMaker : metricMakers) {
            flag = add(metricMaker) || flag;
        }
        return flag;
    }

    @Override
    public String toString() {
        return "MetricMaker Dictionary: " + nameToMetricMaker;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(nameToMetricMaker);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj instanceof MetricMakerDictionary) {
            MetricMakerDictionary that = (MetricMakerDictionary) obj;
            return nameToMetricMaker.equals(that.nameToMetricMaker);
        }
        return false;
    }
}
