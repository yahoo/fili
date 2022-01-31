// Copyright 2022 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.package com.yahoo.bard.webservice.data.cache

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.cache.TimeoutConfigurerBinaryConnectionFactory

import org.junit.rules.Timeout

import net.spy.memcached.DefaultConnectionFactory
import spock.lang.Specification

class TimeoutConfigurerBinaryConnectionFactorySpec extends Specification {

    public static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance()
    public static final String sizeParam = SYSTEM_CONFIG.getPackageVariableName("druid_max_response_length_to_cache")

    String property = "druid_max_response_length_to_cache"

    def setupSpec() {
        SYSTEM_CONFIG.setProperty(sizeParam, "12345")
    }

    def cleanupSpec() {
        SYSTEM_CONFIG.clearProperty(sizeParam)
    }

    def "Controlled parameters are correctly initialized"() {
        setup:
        TimeoutConfigurerBinaryConnectionFactory factory = new TimeoutConfigurerBinaryConnectionFactory()

        expect:
        factory.operationTimeout == DefaultConnectionFactory.DEFAULT_OPERATION_TIMEOUT;
        factory.getDefaultTranscoder().getMaxSize() == 12345
    }
}
