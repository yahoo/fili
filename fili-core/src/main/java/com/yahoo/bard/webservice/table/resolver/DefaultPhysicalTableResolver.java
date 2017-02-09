// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.PartialDataHandler;
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsService;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.ChainingComparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.BinaryOperator;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * A physical table resolver which implements core bard filters and comparator functions
 * <p>
 * {@link PhysicalTable}) based on the optimum (lowest query cost) table, considering
 * completeness of data, granularity, time alignment, aggregatability constraints and cardinality for a particular
 * query.
 */
@Singleton
public class DefaultPhysicalTableResolver extends BasePhysicalTableResolver {

    protected static final GranularityComparator COMPARE_GRANULARITY = new GranularityComparator();
    protected static final DimensionCardinalityComparator CARDINALITY_COMPARATOR = new DimensionCardinalityComparator();

    private final PartialDataHandler partialDataHandler;
    private final VolatileIntervalsService volatileIntervalsService;

    /**
     * Constructor.
     *
     * @param partialDataHandler  Handler for to use for PartialData
     * @param volatileIntervalsService  Service to get volatile intervals from
     */
    @Inject
    public DefaultPhysicalTableResolver(
            PartialDataHandler partialDataHandler,
            VolatileIntervalsService volatileIntervalsService
    ) {
        this.partialDataHandler = partialDataHandler;
        this.volatileIntervalsService = volatileIntervalsService;
    }

    @Override
    public List<PhysicalTableMatcher> getMatchers(QueryPlanningConstraint requestConstraints) {
        SchemaPhysicalTableMatcher schemaMatcher =
                new SchemaPhysicalTableMatcher(requestConstraints);

        TimeAlignmentPhysicalTableMatcher timeAlignmentMatcher =
                new TimeAlignmentPhysicalTableMatcher(requestConstraints);

        AggregatableDimensionsMatcher aggregatabilityMatcher =
                new AggregatableDimensionsMatcher(requestConstraints);

        return Arrays.asList(schemaMatcher, aggregatabilityMatcher, timeAlignmentMatcher);
    }

    @Override
    public BinaryOperator<PhysicalTable> getBetterTableOperator(QueryPlanningConstraint requestConstraints) {
        List<Comparator<PhysicalTable>> comparators = new ArrayList<>();

        if (BardFeatureFlag.PARTIAL_DATA.isOn()) {
            comparators.add(
                    new PartialTimeComparator(requestConstraints, partialDataHandler));
            comparators.add(
                    new VolatileTimeComparator(requestConstraints, partialDataHandler, volatileIntervalsService));
        }
        comparators.add(COMPARE_GRANULARITY);
        comparators.add(CARDINALITY_COMPARATOR);

        ChainingComparator<PhysicalTable> tableComparator = new ChainingComparator<>(comparators);
        return BinaryOperator.minBy(tableComparator);
    }
}
