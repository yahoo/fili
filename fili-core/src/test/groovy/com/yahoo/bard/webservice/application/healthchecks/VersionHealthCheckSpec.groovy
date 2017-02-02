// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.healthchecks

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider

import com.codahale.metrics.health.HealthCheck

import spock.lang.Specification

class VersionHealthCheckSpec extends Specification {

    SystemConfig systemConfig = SystemConfigProvider.getInstance()

    def "Version check is healthy when version configs are defined"() {
        setup:
        // Set expected values
        String knownVersion = "1.2.3.4"
        String knownSha = "b5d0be3956a18e128ff4192ca150f329f5e8f5c1"
        String oldVersion = systemConfig.getStringProperty(VersionHealthCheck.VERSION_KEY, null)
        String oldGitSha = systemConfig.getStringProperty(VersionHealthCheck.GIT_SHA_KEY, null)
        systemConfig.setProperty(VersionHealthCheck.VERSION_KEY, knownVersion)
        systemConfig.setProperty(VersionHealthCheck.GIT_SHA_KEY, knownSha)

        when:
        HealthCheck.Result healthCheck = new VersionHealthCheck().check()

        then: "The check is healthy"
        healthCheck.healthy

        and: "The message is as-expected"
        healthCheck.message == "$knownVersion:$knownSha".toString()

        cleanup:
        systemConfig.resetProperty(VersionHealthCheck.VERSION_KEY, oldVersion)
        systemConfig.resetProperty(VersionHealthCheck.GIT_SHA_KEY, oldGitSha)
    }

    def "Version check is unhealthy when version configs are not defined"() {
        when:
        String versionKey = "AnUnsetKey"
        HealthCheck.Result healthCheck = new VersionHealthCheck(versionKey, versionKey).check()

        then: "The check isn't healthy"
        !healthCheck.healthy

        and: "The message is as-expected"
        healthCheck.message == "$versionKey not set".toString()
    }
}
