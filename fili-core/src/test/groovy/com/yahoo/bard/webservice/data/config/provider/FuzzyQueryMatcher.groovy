// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider

import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation

/**
 * Utility class to validate that TemplateDruidQueries have the same structure and values.
 *
 * It's ok if inner objects don't always have the same names.
 */
public class FuzzyQueryMatcher {

    /**
     * Throws a hopefully-helpful exception if the two queries do not look alike.
     *
     * @param actual the actual output
     * @param fuzzy the expected query
     */
    public static void matches(TemplateDruidQuery actual, TemplateDruidQuery expected) {
        Map<String, Aggregation> expectedAggs = expected.aggregations.collectEntries { [(it.getName()): it] }

        // The number of aggregations should match
        if (actual.aggregations.size() != expectedAggs.size()) {
            throw new RuntimeException("Different agg sizes: expected" + expected.aggregations + "; actual: " + actual.aggregations);
        }

        // Validate that the actual aggregations match
        for (Aggregation actualAggregation : actual.aggregations) {
            if (!expectedAggs.containsKey(actualAggregation.getName())) {
                throw new RuntimeException("Expected to find " + actualAggregation.getName() + "but did not find in expected query");
            }

            validateAggregation(actualAggregation, expectedAggs.get(actualAggregation.getName()));
        }

        Map<String, PostAggregation> expectedPostAggs = expected.postAggregations.collectEntries { [(it.getName()): it] }

        // The number of postAggs should match
        if (actual.postAggregations.size() != expectedPostAggs.size()) {
            throw new RuntimeException("Different postAgg sizes: expected" + expected.postAggregations + "; actual: " + actual.postAggregations);
        }

        for (PostAggregation actualPostAgg : actual.postAggregations) {
            if (!expectedPostAggs.containsKey(actualPostAgg.getName())) {
                throw new RuntimeException("Expected to find " + actualPostAgg.getName() + "but did not find in fuzzy query");
            }

            validatePostAgg(actualPostAgg, expectedPostAggs.get(actualPostAgg.getName()));
        }
    }

    /**
     * Validate that two aggregations are equal
     * @param actual
     * @param expected
     */
    public static void validateAggregation(Aggregation actual, Aggregation expected) {
        if (!(actual.equals(expected))) {
            throw new RuntimeException("Unequal aggregations: expected " + expected + "; actual: " + actual);
        }
    }


    /**
     * Validate that two post aggregations are equal
     * @param actual
     * @param expected
     */
    public static void validatePostAgg(PostAggregation actual, PostAggregation expected) {

        if (actual.getClass() != expected.getClass()) {
            throw new RuntimeException("Expected to find class " + expected.getClass() + "but found " + expected.getClass());
        }

        if (actual instanceof ArithmeticPostAggregation) {
            // Throws exception if don't match
            validateArithmetic((ArithmeticPostAggregation) actual, (ArithmeticPostAggregation) expected);
        } else if (actual instanceof ConstantPostAggregation) {
            validateConstant((ConstantPostAggregation) actual, (ConstantPostAggregation) expected);
        } else if (actual instanceof FieldAccessorPostAggregation) {
            validateFieldAccessor((FieldAccessorPostAggregation) actual,(FieldAccessorPostAggregation) expected);
        }

    }

    /**
     * Validate that two field accessors are equal
     * @param actual
     * @param expected
     */
    public static void validateFieldAccessor(FieldAccessorPostAggregation actual, FieldAccessorPostAggregation expected) {
        if (actual.getFieldName() != expected.getFieldName()) {
            throw new RuntimeException("FieldAccessor field names did not match. Expected: " + expected + "; actual: " + actual);
        }

        validateAggregation(actual.aggregation, expected.aggregation);
    }

    /**
     * Validate that constants are equal
     * @param actual
     * @param expected
     */
    public static void validateConstant(ConstantPostAggregation actual, ConstantPostAggregation expected) {
        if (actual.value != expected.value) {
            throw new RuntimeException("Expected constant=$expected with value=${expected.getValue()}; actual=$actual with value=${actual.getValue()}");
        }
    }

    /**
     * Validate that arithmetic aggregations are equal
     * @param actual
     * @param expected
     */
    public static void validateArithmetic(ArithmeticPostAggregation actual, ArithmeticPostAggregation expected) {
        if (actual.getFn() != expected.getFn()) {
            throw new RuntimeException("Functions do not match; found " + actual.getFn() + "; expected: " + expected.getFn());
        }

        if (!(actual.fields.size() == expected.fields.size())) {
            throw new RuntimeException("Fields do not match in size; found " + actual + "; expected: " + expected);
        }

        for (int i = 0; i < actual.fields.size(); i++) {
            validatePostAgg(actual.fields.get(i), expected.fields.get(i));
        }
    }
}
