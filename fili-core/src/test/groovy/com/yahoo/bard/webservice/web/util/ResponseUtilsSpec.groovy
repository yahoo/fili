// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.web.ResponseFormatType

import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap

import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Path

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.PathSegment
import javax.ws.rs.core.UriInfo

class ResponseUtilsSpec extends Specification {

    ContainerRequestContext containerRequestContext
    List<PathSegment> pathSegmentList
    UriInfo uriInfo
    SystemConfig systemConfig = SystemConfigProvider.getInstance()
    MultivaluedStringMap params

    def setup() {
        containerRequestContext = Mock(ContainerRequestContext)
        uriInfo = Mock(UriInfo)
        PathSegment pathParam1 = Mock(PathSegment)
        PathSegment pathParam2 = Mock(PathSegment)
        pathParam1.getPath() >> "foo"
        pathParam2.getPath() >> "bar"
        pathSegmentList = [pathParam1, pathParam2]
        uriInfo.getPathSegments() >> { pathSegmentList }
        params = new MultivaluedStringMap()

        params.put("dateTime", ["2017/2018"])
        uriInfo.getQueryParameters() >> { params }
        containerRequestContext = Mock(ContainerRequestContext)
        containerRequestContext.getUriInfo() >> uriInfo
        println(containerRequestContext)
    }

    // tests on deprecated getCsvContentDispositionValue method

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

    // tests on non deprecated methods

    @Unroll
    def "default filename is properly built from container request context"() {
        given:
        ResponseUtils responseUtils = new ResponseUtils()
        pathSegmentList = pathSegments.collect {
            it ->
                PathSegment segment = Mock(PathSegment)
                segment.getPath() >> it
                return segment
        }
        params = new MultivaluedStringMap()
        paramTuples.each {
            it -> it = (Tuple) it
                params.put(
                        (String) it.get(0),
                        (List<String>) it.get(1)
                )
        }

        expect:
        responseUtils.prepareDefaultFileNameNoExtension(
                containerRequestContext
        ) == expectedResult

        where:
        expectedResult                          |   pathSegments            |   paramTuples
        ""                                      |   []                      |   []  // this case should never actually occur
        "foo-bar"                               |   ["foo", "bar"]          |   []
        "foo-bar_2017_2018"                     |   ["foo", "bar"]          |   [new Tuple("dateTime", ["2017/2018"])]
        "foo-bar_2017-01-01_2018-01-01"         |   ["foo", "bar"]          |   [new Tuple("dateTime", ["2017-01-01/2018-01-01"])]
        "foo-bar-baz_2017-01-01_2018-01-01"     |   ["foo", "bar", "baz"]   |   [new Tuple("dateTime", ["2017-01-01/2018-01-01"])]
    }

    @Unroll
    def "Long filename is properly truncated when #desc"() {
        setup:
        String baseString = "1234567890"
        String longString = ""
        5.times {
            longString = longString + baseString
        }

        when:
        systemConfig.setProperty(ResponseUtils.MAX_NAME_LENGTH, maxLenProperty)
        ResponseUtils responseUtils = new ResponseUtils()

        then:
        responseUtils.truncateFilePath(longString).length() == expectedLen

        cleanup:
        systemConfig.clearProperty(ResponseUtils.MAX_NAME_LENGTH)

        where:
        maxLenProperty  |   expectedLen | desc
        "20"            |   20          | "max length is less than filename length; filename is truncated to max length"
        "70"            |   50          | "max length is greater than filename length; filename is not truncated"
        "0"             |   50          | "max length is not set (default to 0); filename is not truncated"
    }

    @Unroll
    def "properly builds Content-Disposition header when #desc"() {
        setup:
        String baseString = "1234567890"
        String longString = ""
        ((int) filenameMultiplier).times {
            longString = longString + baseString
        }
        Optional<String> fileName = Optional.ofNullable(longString == "" ? null : longString)

        ResponseFormatType responseFormatType = Mock(ResponseFormatType)
        responseFormatType.getFileExtension() >> extension

        systemConfig.setProperty(ResponseUtils.MAX_NAME_LENGTH, maxFilenameLength)
        ResponseUtils responseUtils = new ResponseUtils()

        when:
        String result = responseUtils.getContentDispositionValue(containerRequestContext, fileName, responseFormatType)

        then:
        result.startsWith(ResponseUtils.CONTENT_DISPOSITION_HEADER_PREFIX)

        when:
        String resultNoPrefix = result.substring(ResponseUtils.CONTENT_DISPOSITION_HEADER_PREFIX.length())

        int expectedFilenameLength
        if (Integer.parseInt(maxFilenameLength) > 0) {
            expectedFilenameLength = Math.min(resultNoPrefix.length() - extension.length(), Integer.parseInt(maxFilenameLength))
        } else {
            expectedFilenameLength = resultNoPrefix.length() - extension.length()
        }
        String resultFileName = resultNoPrefix.substring(0, expectedFilenameLength)
        String resultExtension = resultNoPrefix.substring(resultFileName.length())

        then:
        resultFileName == expectedFilename
        resultExtension == extension

        where:
        filenameMultiplier  |   extension   |   maxFilenameLength                                                           |   expectedFilename                                                                                            |   desc
        5                   |   ".txt"      |   "0"                                                                         |   "1234567890" * 5                                                                                            |   "no max file length"
        2                   |   ".txtttttt" |   "50"                                                                        |   "1234567890" * 2                                                                                            |   "max filename length greater than provided filename size; filename is NOT truncated"
        5                   |   ".txtttttt" |   "20"                                                                        |   "1234567890" * 2                                                                                            |   "max filename length less than provided filename size; filename is truncated to max length"
        5                   |   ".txt"      |   (ResponseUtils.CONTENT_DISPOSITION_HEADER_PREFIX.length() - 1).toString()   |   "123456789012345678901234567890".substring(0, ResponseUtils.CONTENT_DISPOSITION_HEADER_PREFIX.length() - 1) |   "max filename length less than attachment prefix, but prefix doesn't get truncated"
        0                   |   ".txt"      |   "0"                                                                         |   "foo-bar_2017_2018"                                                                                         |   "filename is empty and no max filename length, default filename is used instead and is not truncated"
        0                   |   ".txt"      |   "5"                                                                         |   "foo-b"                                                                                                     |   "filename is empty, default filename is greater than max filename size, default is truncated"
    }
}
