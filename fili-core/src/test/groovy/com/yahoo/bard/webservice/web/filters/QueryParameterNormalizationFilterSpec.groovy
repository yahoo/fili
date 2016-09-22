// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters

import com.google.common.reflect.ClassPath
import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.web.PATCH
import com.yahoo.bard.webservice.web.endpoints.TestFilterServlet
import org.apache.commons.lang3.text.WordUtils
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.DELETE
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.QueryParam
import java.lang.reflect.Method
import javax.servlet.ServletContext

class QueryParameterNormalizationFilterSpec extends Specification {

    @Shared
    JerseyTestBinder jtb

    @Shared
    ServletContext context

    def setupSpec() {
        context = Mock(ServletContext)
        context.getInitParameter("jersey.config.server.provider.packages") >> "com.yahoo.bard.webservice.web.endpoints"
        context.getClassLoader() >> ClassLoader.getSystemClassLoader()
        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(QueryParameterNormalizationFilter.class, TestFilterServlet.class)
    }

    def cleanupSpec() {
        jtb.tearDown()
    }

    @Unroll
    def "check param value is properly echo'd with param name #name"() {
        setup:
        String response = jtb
                .getHarness()
                .target("test/echoParam")
                .queryParam(name, "MyParam")
                .queryParam("test", "abc")
                .request()
                .get(String.class)

        expect:
        response == "MyParam"

        where:
        // The possibilities are small... May as well calculate.
        name << wordCasingCombinations("testingecho")

    }

    def "check that valid endpoints are considered HTTP endpoints"() {
        setup:
        Method get = InnerClassWithAnnotatedMethods.class.getMethod("methodGet")
        Method post = InnerClassWithAnnotatedMethods.class.getMethod("methodPost")
        Method delete = InnerClassWithAnnotatedMethods.class.getMethod("methodDelete")
        Method patch = InnerClassWithAnnotatedMethods.class.getMethod("methodPatch")

        expect:
        QueryParameterNormalizationFilter.isWebEndpoint(get)
        QueryParameterNormalizationFilter.isWebEndpoint(post)
        QueryParameterNormalizationFilter.isWebEndpoint(delete)
        QueryParameterNormalizationFilter.isWebEndpoint(patch)
    }

    def "check invalid endpoints are not considered HTTP endpoints"() {
        setup:
        Method noAnnotation = InnerClassWithAnnotatedMethods.class.getMethod("nonAnnotationMethod")
        Method inertAnnotation = InnerClassWithAnnotatedMethods.class.getMethod("inertMethod")

        expect:
        !QueryParameterNormalizationFilter.isWebEndpoint(noAnnotation)
        !QueryParameterNormalizationFilter.isWebEndpoint(inertAnnotation)
    }

    def "check valid case mapping and function finding"() {
        setup:
        Set<Class<?>> classes = new HashSet<>()
        ClassPath.from(ClassLoader.getSystemClassLoader()).getTopLevelClasses("com.yahoo.bard.webservice.web.endpoints").each {
            classes.add(it.load())
        }
        def map = QueryParameterNormalizationFilter.buildParameterMap(classes, ClassLoader.getSystemClassLoader())

        expect:
        map.containsKey("topn")
        map.containsKey("testingecho")
        map.containsKey("having")
        map.get("topn") == "topN"
        map.get("testingecho") == "tEstingEChO"
        map.get("having") == "having"
    }

    def "check fails on mismatched casing duplicates"() {
        when:
        // This package has an endpoint that exists because of the inner class endpoint we've created here
        QueryParameterNormalizationFilter.buildParameterMap([ExampleDuplicateMapping] as Set, ClassLoader.getSystemClassLoader())

        then:
        thrown RuntimeException
    }

    def wordCasingCombinations(String word) {
        String current = word.toLowerCase(Locale.ENGLISH)
        Set<String> words = []
        int length = current.size()
        // Meh, quick/naive impl prioritized over clean and elegant here... Sorry.
        // Generate many upper- and lowercase combinations of the parameter
        for (int i = 0 ; i < length ; ++i) {
            words.add(current)
            for (int j = (i + 1) % length ; i != j ; j = (j + 1) % length) {
                current = updateCase(current, j)
                words.add(current)
            }
            current = updateCase(current, i)
            words.add(current)
            for (int j = (i + 1) % length ; i != j ; j = (j + 1) % length) {
                current = updateCase(current, j)
                words.add(current)
            }
        }
        return words
    }

    def updateCase(String word, int j) {
        String theChar = WordUtils.swapCase(word.substring(j, j + 1))
        return word.substring(0, j) + theChar + word.substring(j + 1)
    }


    class InnerClassWithAnnotatedMethods {
        @GET
        public void methodGet() {}

        @POST
        public void methodPost() {}

        @DELETE
        public void methodDelete() {}

        @PATCH
        public void methodPatch() {}

        public void nonAnnotationMethod() {}

        @Path("/yay")
        public void inertMethod() {}
    }

    @Path("/duplicateMapping")
    class ExampleDuplicateMapping {
        @GET
        public String test(@QueryParam("abc") String abc) {
            return abc;
        }

        @POST
        public String testPost(@QueryParam("aBc") String abc) {
            return abc;
        }
    }
}
