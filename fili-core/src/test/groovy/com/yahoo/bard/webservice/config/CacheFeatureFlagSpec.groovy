// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config

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
        SYSTEM_CONFIG.clearProperty(TTL_CACHE_CONFIG_KEY)
        SYSTEM_CONFIG.clearProperty(LOCAL_SIGNATURE_CACHE_CONFIG_KEY)
        SYSTEM_CONFIG.clearProperty(ETAG_CACHE_CONFIG_KEY)

        // restore config value
        SYSTEM_CONFIG.setProperty(ETAG_CACHE_CONFIG_KEY, queryResponseCachingStrategy)
    }

    @Unroll
    def "When DruidCache is #ttlIsOn and DruidCacheV2 is #localSignatureIsOn then expect #expected"() {
        setup:
        def badValues = [NONE, TTL, LOCAL_SIGNATURE, ETAG]
        badValues.remove(expected)
        SYSTEM_CONFIG.setProperty(TTL_CACHE_CONFIG_KEY, String.valueOf(ttlIsOn))
        SYSTEM_CONFIG.setProperty(LOCAL_SIGNATURE_CACHE_CONFIG_KEY, String.valueOf(localSignatureIsOn))

        expect:
        expected.isOn()
        badValues.each {
            assert ! it.isOn()
        }

        cleanup:
        SYSTEM_CONFIG.clearProperty(TTL_CACHE_CONFIG_KEY)
        SYSTEM_CONFIG.clearProperty(LOCAL_SIGNATURE_CACHE_CONFIG_KEY)

        where:
        ttlIsOn | localSignatureIsOn | NoCacheOn | expected
        true    | false              | false     | TTL
        false   | true               | false     | NONE
        true    | true               | false     | LOCAL_SIGNATURE
        false   | false              | true      | NONE
    }
}
