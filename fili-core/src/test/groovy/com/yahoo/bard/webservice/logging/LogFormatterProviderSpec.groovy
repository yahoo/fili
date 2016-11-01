package com.yahoo.bard.webservice.logging

import static com.yahoo.bard.webservice.logging.LogFormatterProvider.LOG_FORMATTER_IMPLEMENTATION_SETTING_NAME

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigException
import com.yahoo.bard.webservice.config.SystemConfigProvider

import spock.lang.Shared
import spock.lang.Specification


class LogFormatterProviderSpec extends Specification {

    @Shared LogFormatter originalLogFormatter
    String originalLogFormatterClassName
    SystemConfig systemConfig = SystemConfigProvider.instance;
    String logFormatterKey = systemConfig.getPackageVariableName(LOG_FORMATTER_IMPLEMENTATION_SETTING_NAME)

    def setupSpec() {
        originalLogFormatter = LogFormatterProvider.logFormatter
    }

    def cleanupSpec() {
        LogFormatterProvider.logFormatter = originalLogFormatter
    }

    def setup() {
        LogFormatterProvider.logFormatter = null
        try {
            originalLogFormatterClassName = systemConfig.getStringProperty(logFormatterKey)
        } catch(SystemConfigException ignored) {
            originalLogFormatterClassName = null
        }
    }

    def cleanup() {
        if (originalLogFormatterClassName == null) {
            systemConfig.clearProperty(logFormatterKey)
        } else {
            systemConfig.setProperty(logFormatterKey, originalLogFormatterClassName)
        }
    }

    def "The LogFormatterProvider instantiates the configured logFormatter instance"() {
        expect: "We haven't instantiated the log formatter yet"
        LogFormatterProvider.logFormatter == null

        when:
        systemConfig.setProperty(logFormatterKey, TestLogFormatter.class.name)

        then:
        LogFormatterProvider.instance instanceof TestLogFormatter
    }

    def "When the LogFormatterProvider is not specified, we default to the JsonLogFormatter"() {
        expect: "We haven't instantiated the log formatter yet"
        LogFormatterProvider.logFormatter == null

        when:
        systemConfig.clearProperty(logFormatterKey)

        then:
        LogFormatterProvider.instance instanceof JsonLogFormatter
    }

}
