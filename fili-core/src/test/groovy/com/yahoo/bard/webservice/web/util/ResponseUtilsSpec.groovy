// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider

import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap

import spock.lang.Specification

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.PathSegment
import javax.ws.rs.core.UriInfo

class ResponseUtilsSpec extends Specification {

    ContainerRequestContext containerRequestContext
    List<PathSegment> pathSegmentList
    UriInfo uriInfo
    SystemConfig systemConfig = SystemConfigProvider.getInstance()

    def setup() {
        containerRequestContext = Mock(ContainerRequestContext)
        uriInfo = Mock(UriInfo)
        PathSegment pathParam1 = Mock(PathSegment)
        PathSegment pathParam2 = Mock(PathSegment)
        pathParam1.getPath() >> "foo"
        pathParam2.getPath() >> "bar"
        pathSegmentList = [pathParam1, pathParam2]
        uriInfo.getPathSegments() >> pathSegmentList
        MultivaluedStringMap params = new MultivaluedStringMap()

        params.put("dateTime", ["2017/2018"])
        uriInfo.getQueryParameters() >> params
        containerRequestContext = Mock(ContainerRequestContext)
        containerRequestContext.getUriInfo() >> uriInfo
        println(containerRequestContext)
    }

    def "Simple CSV header translates appropriately"() {
        given:
        ResponseUtils responseUtils = new ResponseUtils()

        expect:
        responseUtils.getCsvContentDispositionValue(
                containerRequestContext
        ) == "attachment; filename=foo-bar_2017_2018.csv"

    }


    def "Long CSV header is truncated or not based on config"() {
        setup:
        String baseString = "1234567890"
        String longString = ""
        5.times {
            longString = longString + baseString
        }

        and:
        int baseLength = "attachment; filename=.csv".length()

        and:

        PathSegment pathSegment = Mock(PathSegment)
        pathSegment.getPath() >> longString
        pathSegmentList.add(pathSegment)

        when:
        systemConfig.setProperty(ResponseUtils.MAX_NAME_LENGTH, "20")
        ResponseUtils responseUtils = new ResponseUtils()

        then:
        responseUtils.getCsvContentDispositionValue(
                containerRequestContext
        ).length() == 20 + baseLength
        responseUtils.getCsvContentDispositionValue(
                containerRequestContext
        ).endsWith(".csv")


        when:
        systemConfig.setProperty(ResponseUtils.MAX_NAME_LENGTH, "0")
        responseUtils = new ResponseUtils()

        then:
        responseUtils.getCsvContentDispositionValue(
                containerRequestContext
        ) == "attachment; filename=foo-bar-" + longString +"_2017_2018.csv"

        cleanup:
        systemConfig.clearProperty(ResponseUtils.MAX_NAME_LENGTH)
    }
}
