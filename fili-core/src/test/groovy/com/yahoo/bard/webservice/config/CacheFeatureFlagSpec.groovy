// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config

import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE
import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE_V2
import static com.yahoo.bard.webservice.config.CacheFeatureFlag.*

import spock.lang.Specification
import spock.lang.Unroll

class CacheFeatureFlagSpec extends Specification {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance()
    private static final String TTL_CACHE_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName("druid_cache_enabled")
    private static final String LOCAL_SIGNATURE_CACHE_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName("druid_cache_v2_enabled")
    private static final String ETAG_CACHE_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName("query_response_caching_strategy")

    String queryResponseCachingStrategy

    def setup() {
        // store config value
        queryResponseCachingStrategy = SYSTEM_CONFIG.getStringProperty(ETAG_CACHE_CONFIG_KEY, "NoCache")
    }

    def cleanup() {
        CacheFeatureFlag.TTL.reset()
        CacheFeatureFlag.ETAG.reset()
        CacheFeatureFlag.LOCAL_SIGNATURE.reset()
        CacheFeatureFlag.NONE.reset()
    }

    @Unroll
    def "When DruidCache is #ttlIsOn and DruidCacheV2 is #localSignatureIsOn then expect #expected"() {
        setup:
        def badValues = [NONE, TTL, LOCAL_SIGNATURE, ETAG]
        badValues.remove(expected)
        DRUID_CACHE.setOn(ttlIsOn)
        DRUID_CACHE_V2.setOn(localSignatureIsOn)
        expected.reset()

        expect:
        expected.isOn()
        badValues.each {
            it.reset()
            assert ! it.isOn()
        }

        cleanup:
        DRUID_CACHE.reset()
        DRUID_CACHE_V2.reset()

        where:
        ttlIsOn | localSignatureIsOn | NoCacheOn | expected
        true    | false              | false     | TTL
        false   | true               | false     | NONE
        true    | true               | false     | LOCAL_SIGNATURE
        false   | false              | true      | NONE
    }
}
