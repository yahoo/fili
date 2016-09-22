// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider

import spock.lang.Shared
import spock.lang.Specification

class DruidClientConfigHelperSpec extends Specification {

    private static final SystemConfig systemConfig = SystemConfigProvider.getInstance()

    private static final String UI_URL_SETTING_KEY =
            systemConfig.getPackageVariableName("ui_druid_broker");

    private static final String NON_UI_URL_SETTING_KEY = systemConfig.getPackageVariableName("non_ui_druid_broker");

    private static final String UI_DRUID_REQUEST_TIMEOUT_KEY = systemConfig.getPackageVariableName(
            "ui_druid_request_timeout"
    );

    private static final String NON_UI_DRUID_REQUEST_TIMEOUT_KEY =
        systemConfig.getPackageVariableName("non_ui_druid_request_timeout");

    private static final String expectedUiUrl = "http://ui-broker"
    private static final String expectedNonUiUrl = "http://nonui-broker"

    private static final String expectedUiRequestTimeout ="600000"
    private static final String expectedNonUiRequestTimeout ="300000"

    @Shared def uiUrl
    @Shared def nonUiUrl
    @Shared def uiRequestTimeout
    @Shared def nonUiRequestTimeout

    def setupSpec() {
        uiUrl = systemConfig.getStringProperty(UI_URL_SETTING_KEY)
        nonUiUrl = systemConfig.getStringProperty(NON_UI_URL_SETTING_KEY)
        assert uiUrl != null : "Property: " + UI_URL_SETTING_KEY
        assert nonUiUrl != null : "Property: " + NON_UI_URL_SETTING_KEY

        uiRequestTimeout = systemConfig.getStringProperty(UI_DRUID_REQUEST_TIMEOUT_KEY, null)
        if (uiRequestTimeout == null) {
            systemConfig.setProperty(UI_DRUID_REQUEST_TIMEOUT_KEY, expectedUiRequestTimeout)
        }

        nonUiRequestTimeout = systemConfig.getStringProperty(NON_UI_DRUID_REQUEST_TIMEOUT_KEY, null)
        if (nonUiRequestTimeout == null) {
            systemConfig.setProperty(NON_UI_DRUID_REQUEST_TIMEOUT_KEY, expectedNonUiRequestTimeout)
        }
    }

    def cleanupSpec() {
        if (uiUrl == null) {
            systemConfig.clearProperty(UI_URL_SETTING_KEY)
        } else {
            systemConfig.setProperty(UI_URL_SETTING_KEY , uiUrl)
        }

        if (nonUiUrl == null) {
            systemConfig.clearProperty(NON_UI_URL_SETTING_KEY)
        } else {
            systemConfig.setProperty(NON_UI_URL_SETTING_KEY , nonUiUrl)
        }

        if (uiRequestTimeout == null) {
            systemConfig.clearProperty(UI_DRUID_REQUEST_TIMEOUT_KEY)
        } else {
            systemConfig.setProperty(UI_DRUID_REQUEST_TIMEOUT_KEY , uiRequestTimeout)
        }

        if (nonUiRequestTimeout == null) {
            systemConfig.clearProperty(NON_UI_DRUID_REQUEST_TIMEOUT_KEY)
        } else {
            systemConfig.setProperty(NON_UI_DRUID_REQUEST_TIMEOUT_KEY , nonUiRequestTimeout)
        }
    }

    def "check if appropriate UI druid broker url is fetched"() {
        expect:
        DruidClientConfigHelper.getDruidUiUrl() == expectedUiUrl
    }

    def "check if appropriate non-UI druid broker url is fetched"() {
        expect:
        DruidClientConfigHelper.getDruidNonUiUrl() == expectedNonUiUrl
    }

    def "check if appropriate UI druid request timeout is fetched"() {
        expect:
        DruidClientConfigHelper.getDruidUiTimeout() == Integer.parseInt(expectedUiRequestTimeout)
    }

    def "check if appropriate non-UI druid request timeout is fetched"() {
        expect:
        DruidClientConfigHelper.getDruidNonUiTimeout() == Integer.parseInt(expectedNonUiRequestTimeout)
    }
}
