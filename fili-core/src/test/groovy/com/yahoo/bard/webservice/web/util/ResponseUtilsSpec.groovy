// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.web.DefaultResponseFormatType
import com.yahoo.bard.webservice.web.ResponseFormatType

import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap

import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.HttpHeaders
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

    def "getting content type header value successfully concatenates the values provided to it by the response format type"() {
        setup:
        ResponseFormatType formatType = Mock(ResponseFormatType)
        formatType.getContentType() >> "hello/world"
        formatType.getCharset() >> "utf-8"

        when:
        String result = new ResponseUtils().getContentTypeValue(formatType)

        then:
        result == "hello/world; charset=utf-8"
    }

    def "replaceReservedCharacters replaces '/' with '_' and ',' with '__'"() {
        setup:
        String input = "\\h\\e/l/l//o_wor,l,d,"
        String expectedOutput = "_h_e_l_l__o_wor__l__d__"

        when:
        String result = new ResponseUtils().replaceReservedCharacters(input)

        then:
        result == expectedOutput
    }

    @Unroll
    def "Filename #inputFilename with response format type csv #is truncated, where expected filename = #expected"() {
        expect:
        new ResponseUtils().removeDuplicateExtensions(containerRequestContext, "SOMETHING.csv", DefaultResponseFormatType.CSV) == "SOMETHING"

        where:
        inputFilename           | is        || expected
        "SOMETHING.csv"         | "is"      || "SOMETHING"
        "something.CsV"         | "is"      || "something"
        "something.csv.csv.csv" | "is"      || "something"
        ".csv.csv.csv"          | "is"      || "foo-bar_2017_2018"
        "something"             | "is NOT"  || "something"
        "SOMETHING.json"        | "is NOT"  || "SOMETHING.json"
        ".json"                 | "is NOT"  || ".json"
        "hello.csv.world"       | "is NOT"  || "hello.csv.world"
        "somethingcsv"          | "is NOT"  || "somethingcsv"
    }

    def "Filenames that end with file extensions that match the response format's file extension have the file extension truncated"() {
        setup:
        String filename = "filename.json.json"
        ResponseFormatType responseFormat = DefaultResponseFormatType.JSON

        expect:
        new ResponseUtils().getContentDispositionValue(containerRequestContext, filename, responseFormat) == "attachment; filename=filename.json"
    }

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
        responseUtils.generateDefaultFileNameNoExtension(
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
        responseUtils.truncateFilename(longString).length() == expectedLen

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
        String filename
        def expectedFilenameLength

        if (filenameMultiplier >= 0) {
            String baseString = "1234567890"
            String longString = ""
            ((int) filenameMultiplier).times {
                longString = longString + baseString
            }
            filename = longString
        } else {
            filename = null
        }

        ResponseFormatType responseFormatType = Mock(ResponseFormatType)
        responseFormatType.getFileExtension() >> extension

        systemConfig.setProperty(ResponseUtils.MAX_NAME_LENGTH, maxFilenameLength)
        ResponseUtils responseUtils = new ResponseUtils()

        when:
        String result = responseUtils.getContentDispositionValue(containerRequestContext, filename, responseFormatType)

        then:
        result.startsWith(ResponseUtils.CONTENT_DISPOSITION_HEADER_PREFIX)

        when:
        String resultNoPrefix = result.substring(ResponseUtils.CONTENT_DISPOSITION_HEADER_PREFIX.length())

        if (Integer.parseInt(maxFilenameLength) > 0) {
            expectedFilenameLength = Math.min(
                    resultNoPrefix.length() - extension.length(),
                    Integer.parseInt(maxFilenameLength)
            )
        } else {
            expectedFilenameLength = resultNoPrefix.length() - extension.length()
        }
        String resultFileName = resultNoPrefix.substring(0, expectedFilenameLength)
        String resultExtension = resultNoPrefix.substring(resultFileName.length())

        then:
        resultFileName == expectedFilename
        resultExtension == extension

        cleanup:
        systemConfig.clearProperty(ResponseUtils.MAX_NAME_LENGTH)

        where:
        filenameMultiplier | extension   | maxFilenameLength                                                         | expectedFilename    | desc
        5                  | ".txt"      | "0"                                                                       | "1234567890" * 5    | "no max file length"
        2                  | ".txtttttt" | "50"                                                                      | "1234567890" * 2    | "max filename length greater than provided filename size; filename is NOT truncated"
        5                  | ".txtttttt" | "20"                                                                      | "1234567890" * 2    | "max filename length less than provided filename size; filename is truncated to max length"
        5                  | ".txt"      | (ResponseUtils.CONTENT_DISPOSITION_HEADER_PREFIX.length() - 1).toString() | "123456789012345678901234567890".substring(0, ResponseUtils.CONTENT_DISPOSITION_HEADER_PREFIX.length() - 1) | "max filename length less than attachment prefix, but prefix doesn't get truncated"
        0                  | ".txt"      | "0"                                                                       | "foo-bar_2017_2018" | "filename is empty and no max filename length, default filename is used instead and is not truncated"
        0                  | ".txt"      | "5"                                                                       | "foo-b"             | "filename is empty, default filename is greater than max filename size, default is truncated"
        -1                 | ".txt"      | "0"                                                                       | "foo-bar_2017_2018" | "filename is null and no max filename length, default filename is used instead and is not truncated"
        -1                 | ".txt"      | "5"                                                                       | "foo-b"             | "filename is null, default filename is greater than max filename size, default is truncated"
    }

    @Unroll
    def "no filename is specified, so default is used and #desc"() {
        setup:
        ResponseFormatType responseFormatType = Mock(ResponseFormatType)
        responseFormatType.getFileExtension() >> extension

        systemConfig.setProperty(ResponseUtils.MAX_NAME_LENGTH, maxFilenameLength)
        ResponseUtils responseUtils = new ResponseUtils()

        when:
        String result = responseUtils.getContentDispositionValue(containerRequestContext, responseFormatType)

        then:
        result.startsWith(ResponseUtils.CONTENT_DISPOSITION_HEADER_PREFIX)

        when:
        String resultNoPrefix = result.substring(ResponseUtils.CONTENT_DISPOSITION_HEADER_PREFIX.length())

        def expectedFilenameLength
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

        cleanup:
        systemConfig.clearProperty(ResponseUtils.MAX_NAME_LENGTH)

        where:
        extension   |   maxFilenameLength   |   expectedFilename    |   desc
        ".txt"      |   "0"                 |   "foo-bar_2017_2018" |   "there is no max filename length so default filename is not truncated"
        ".txt"      |   "5"                 |   "foo-b"             |   "the default filename is greater than max filename size, so default is truncated"
    }

    def "generating response format headers with null, empty, or no provided filename does not generate content disposition header"() {
        setup:
        ResponseFormatType responseFormatType = Mock(ResponseFormatType)
        responseFormatType.getContentType() >> "hello/world"
        responseFormatType.getCharset() >> "utf-8"

        ResponseUtils responseUtils = new ResponseUtils()

        String expectedContentTypeValue = "hello/world; charset=utf-8"

        when:
        Map<String, String> result = responseUtils.buildResponseFormatHeaders(Mock(ContainerRequestContext), responseFormatType)

        then:
        result.keySet().size() == 1
        result.containsKey(HttpHeaders.CONTENT_TYPE)
        result.get(HttpHeaders.CONTENT_TYPE) == expectedContentTypeValue

        when:
        result = responseUtils.buildResponseFormatHeaders(Mock(ContainerRequestContext), null, responseFormatType)

        then:
        result.keySet().size() == 1
        result.containsKey(HttpHeaders.CONTENT_TYPE)
        result.get(HttpHeaders.CONTENT_TYPE) == expectedContentTypeValue

        when:
        result = responseUtils.buildResponseFormatHeaders(Mock(ContainerRequestContext), "", responseFormatType)

        then:
        result.keySet().size() == 1
        result.containsKey(HttpHeaders.CONTENT_TYPE)
        result.get(HttpHeaders.CONTENT_TYPE) == expectedContentTypeValue
    }

    def "when nonempty filename is provided the content disposition header is present"() {
        setup:
        ResponseFormatType responseFormatType = Mock(ResponseFormatType)
        responseFormatType.getContentType() >> "hello/world"
        responseFormatType.getCharset() >> "utf-8"
        responseFormatType.getFileExtension() >> ".txt"

        String fileName = "f,na/me"

        ResponseUtils responseUtils = new ResponseUtils()

        String expectedContentTypeValue = "hello/world; charset=utf-8"
        String expectedContentDispositionValue = "attachment; filename=f__na_me.txt"

        when:
        Map<String, String> result = responseUtils.buildResponseFormatHeaders(Mock(ContainerRequestContext), fileName, responseFormatType)

        then:
        result.keySet().size() == 2
        result.containsKey(HttpHeaders.CONTENT_TYPE)
        result.containsKey(HttpHeaders.CONTENT_DISPOSITION)
        result.get(HttpHeaders.CONTENT_TYPE) == expectedContentTypeValue
        result.get(HttpHeaders.CONTENT_DISPOSITION) == expectedContentDispositionValue
    }

    def "when empty filename is provided BUT response format is in the always download set, the content disposition header is present"() {
        setup:
        ResponseUtils responseUtils = new ResponseUtils()

        String expectedContentTypeValue = "text/csv; charset=utf-8"
        String expectedContentDispositionValue = "attachment; filename=foo-bar_2017_2018.csv"

        when:
        Map<String, String> result = responseUtils.buildResponseFormatHeaders(containerRequestContext, DefaultResponseFormatType.CSV)

        then:
        result.keySet().size() == 2
        result.containsKey(HttpHeaders.CONTENT_TYPE)
        result.containsKey(HttpHeaders.CONTENT_DISPOSITION)
        result.get(HttpHeaders.CONTENT_TYPE) == expectedContentTypeValue
        result.get(HttpHeaders.CONTENT_DISPOSITION) == expectedContentDispositionValue
    }
}
