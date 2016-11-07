// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config

import com.yahoo.bard.webservice.application.JerseyTestBinder

import org.glassfish.hk2.api.IterableProvider

import spock.lang.Specification
import spock.lang.Unroll

import java.util.stream.Collectors

class FeatureFlagRegistrySpec extends Specification {

    final JerseyTestBinder jtb = new JerseyTestBinder()
    final IterableProvider<FeatureFlag> iterableProvider = Mock(IterableProvider)
    FeatureFlagRegistry flagRegistry

    def setup() {
        iterableProvider.spliterator() >> {
            jtb.testBinderFactory.collectFeatureFlags(BardFeatureFlag).spliterator()
        }
        flagRegistry = new FeatureFlagRegistry(iterableProvider)
    }

    def cleanup() {
        jtb.tearDown()
    }

    def "Every enum value maps to a known feature flag"() {
        when:
        def values = flagRegistry.getValues().stream()
                .map { v -> v.getName() }
                .collect(Collectors.toSet()) as Set

        then:
        values == ["partial_data_enabled", "druid_cache_enabled", "druid_cache_v2_enabled", "query_split_enabled",
                   "top_n_enabled", "data_filter_substring_operations_enabled", "intersection_reporting_enabled",
                   "permissive_column_availability_enabled", "updated_metadata_collection_names_enabled",
                   "druid_coordinator_metadata_enabled", "druid_dimensions_loader_enabled",
                   "case_sensitive_keys_enabled"] as Set
    }

    @Unroll
    def "Feature flag #flagName has a valid enum value"() {
        expect:
        flagRegistry.forName(flagName) instanceof FeatureFlag

        where:
        flagName << ["partial_data_enabled", "druid_cache_enabled", "druid_cache_v2_enabled", "query_split_enabled",
                     "top_n_enabled", "data_filter_substring_operations_enabled", "intersection_reporting_enabled",
                     "permissive_column_availability_enabled", "updated_metadata_collection_names_enabled",
                     "druid_coordinator_metadata_enabled", "druid_dimensions_loader_enabled",
                     "case_sensitive_keys_enabled"]
    }
}
