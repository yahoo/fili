// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.WithMetricField;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.WithPostAggregations;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Static utility methods that support interacting with {@link TemplateDruidQuery} instances.
 */
public final class TemplateDruidQueryUtils {

    /**
     * Private constructor. This is a utility class, should NEVER instantiate it.
     *
     * @throws AssertionError if this constructor is somehow called, through reflection or some other method.
     */
    private TemplateDruidQueryUtils() {
        throw new AssertionError("Static utility class, should never instantiate");
    }

    /**
     * Updates any reference to {@code oldField} on {@code fieldToCheck} to point to {@code newField}. If fieldToCheck
     * is equivalent to oldField, newField is returned. This method may return either fieldToCheck or an equivalent copy
     * built from the public {@code with} methods on MetricField
     * <p>
     * This method relies on proper implementation of "copy with modification" methods on all parsed MetricField
     * implementations. If a MetricField implementation does not fulfil all of the contracts described in the
     * MetricField and related class Javadocs, this method is not guaranteed to function properly.
     * <p>
     * Currently Fili, and by extension this method, recognizes the {@link WithMetricField} and
     * {@link WithPostAggregations} interfaces as the only way for {@link MetricField} implementations to publish that
     * they depend on other fields. Any MetricField implementation that depends on other metric fields MUST also
     * implement one of those interfaces. Those interfaces currently account for all MetricField types supported by
     * Fili, and almost all cases supported by Druid (with JavascriptAggregation being the notable exception). More
     * cases may be supported in the future, and this method will be expanded to handle those cases.
     *
     * @param <T>  The type of field that is being replaced
     * @param fieldToCheck  The MetricField to be examined and repointed
     * @param oldField  The original MetricField that needs to be replaced with newField
     * @param newField  The new MetricField that is replacing oldField
     * @return either newField if fieldToCheck itself needs to be replaced, or a copy of fieldToCheck that has any
     *         references to oldField replaced with references to newField
     */
    public static <T extends MetricField> MetricField repointToNewMetricField(
            MetricField fieldToCheck,
            T oldField,
            T newField
    ) {
        if (Objects.equals(oldField, fieldToCheck)) {
            return newField;
        }

        // if has children, iterate through children and repoint
        if (fieldToCheck instanceof WithPostAggregations) {
            WithPostAggregations root = (WithPostAggregations) fieldToCheck;


            return root.withPostAggregations(
                    root.getPostAggregations().stream()
                            .map(pa -> repointToNewMetricField(pa, oldField, newField))
                            // This cast is safe because the only location where a type change can occur is in the
                            // WithPostAggregations#withPostAggregations method call. The contract on that method
                            // requires implementors that also subclass Aggregation or PostAggregation return a subclass
                            // of the implemented type. For example, PostAggregation#withPostAggregations must return a
                            // PostAggregation. Clients that breaks this contract will have a ClassCastException thrown
                            // on this line. Since this is a stream of post aggregations, this contract holds for all
                            // cases.
                            .map(mf -> (PostAggregation) mf)
                            .collect(Collectors.toList())
            );
        }
        if (fieldToCheck instanceof WithMetricField) {
            WithMetricField root = (WithMetricField) fieldToCheck;
            return root.withMetricField(repointToNewMetricField(root.getMetricField(), oldField, newField));
        }

        return fieldToCheck;
    }
}
