// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config

import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Unroll

class CacheFeatureFlagSpec extends Specification {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance()
    private static final String TTL_CACHE_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName("druid_cache_enabled")
    private static final String LOCAL_SIGNATURE_CACHE_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName("druid_cache_v2_enabled")
    private static final String ETAG_CACHE_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName("query_response_caching_strategy")

    def cleanup() {
        SYSTEM_CONFIG.clearProperty(TTL_CACHE_CONFIG_KEY)
        SYSTEM_CONFIG.clearProperty(LOCAL_SIGNATURE_CACHE_CONFIG_KEY)
        SYSTEM_CONFIG.clearProperty(ETAG_CACHE_CONFIG_KEY)
    }

    @Unroll
    @Ignore
    def "When TTL or local signature cache is set, old caching strategy is applied"() {
        setup:
        SYSTEM_CONFIG.setProperty(TTL_CACHE_CONFIG_KEY, String.valueOf(ttlIsOn))
        SYSTEM_CONFIG.setProperty(LOCAL_SIGNATURE_CACHE_CONFIG_KEY, String.valueOf(localSignatureIsOn))

        expect:
        CacheFeatureFlag.NONE.isOn() == NoCacheOn
        CacheFeatureFlag.TTL.isOn() == ttlOn
        CacheFeatureFlag.LOCAL_SIGNATURE.isOn() == localSignatureOn
        CacheFeatureFlag.ETAG.isOn() == eTagOn

        where:
        ttlIsOn | localSignatureIsOn | NoCacheOn | ttlOn  | localSignatureOn  | eTagOn
        true    | false              | false     | true   | false             | false
        false   | true               | false     | false  | true              | false
        true    | true               | false     | false  | true              | false
        false   | false              | true      | false  | false             | false
    }
}
