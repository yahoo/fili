// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.PartialDataHandler;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.ChainingComparator;
import com.yahoo.bard.webservice.web.DataApiRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.BinaryOperator;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * A physical table resolver which implements core bard filters and comparator functions
 *
 * {@link com.yahoo.bard.webservice.table.PhysicalTable}) based on the optimum (lowest query cost) table, considering
 * completeness of data, granularity, time alignment, aggregatability constraints and cardinality for a particular
 * query.
 */
@Singleton
public class DefaultPhysicalTableResolver extends BasePhysicalTableResolver implements PhysicalTableResolver {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPhysicalTableResolver.class);

    protected static final GranularityComparator COMPARE_GRANULARITY = new GranularityComparator();
    protected static final DimensionCardinalityComparator CARDINALITY_COMPARATOR = new DimensionCardinalityComparator();

    private final PartialDataHandler partialDataHandler;

    @Inject
    public DefaultPhysicalTableResolver(PartialDataHandler partialDataHandler) {
        this.partialDataHandler = partialDataHandler;
    }

    @Override
    public List<PhysicalTableMatcher> getMatchers(DataApiRequest apiRequest, TemplateDruidQuery query) {
        SchemaPhysicalTableMatcher schemaMatcher = new SchemaPhysicalTableMatcher(
                apiRequest,
                query,
                resolveAcceptingGrain.apply(apiRequest, query)
        );
        TimeAlignmentPhysicalTableMatcher timeAlignmentMatcher = new TimeAlignmentPhysicalTableMatcher(apiRequest);
        AggregatableDimensionsMatcher aggregatabilityMatcher = new AggregatableDimensionsMatcher(apiRequest, query);

        return Arrays.asList(schemaMatcher, aggregatabilityMatcher, timeAlignmentMatcher);
    }

    @Override
    public BinaryOperator<PhysicalTable> getBetterTableOperator(DataApiRequest apiRequest, TemplateDruidQuery query) {
        List<Comparator<PhysicalTable>> comparators = new ArrayList<>();

        if (BardFeatureFlag.PARTIAL_DATA.isOn()) {
            comparators.add(new PartialTimeComparator(apiRequest, query, partialDataHandler));
        }
        comparators.add(COMPARE_GRANULARITY);
        comparators.add(CARDINALITY_COMPARATOR);
        ChainingComparator<PhysicalTable> tableComparator = new ChainingComparator<>(comparators);
        return BinaryOperator.minBy(tableComparator);
    }
}
