// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import spock.lang.Ignore
import spock.lang.Timeout

/**
 * Checks that all of the table/grain pairs have the dimensions and columns expected.
 */
@Ignore("Ignoring this test case due to memory leakage issue on build machine. But it works fine during local build")
@Timeout(30)    // Fail test if hangs
class ExpectedTableColumnsEndpointSpec extends BaseTableServletComponentSpec {

    @Override
    Class<?>[] getResourceClasses() {
        [TablesServlet.class]
    }

    @Override
    String getTarget() {
        return "tables/shapes/day"
    }

    @Override
    String getExpectedApiResponse() {
        """{
              "category": "General",
              "name": "shapes",
              "longName": "shapes",
              "timeGrain": "day",
              "retention": "P1Y",
              "description": "shapes",
              "dimensions": [
                {
                  "cardinality": "0",
                  "category": "General",
                  "name": "color",
                  "longName": "color",
                  "uri": "http://localhost:9998/dimensions/color"
                }, {
                  "cardinality": "0",
                  "category": "General",
                  "name": "shape",
                  "longName": "shape",
                  "uri": "http://localhost:9998/dimensions/shape"
                }, {
                  "cardinality": "0",
                  "category": "General",
                  "name": "size",
                  "longName": "size",
                  "uri": "http://localhost:9998/dimensions/size"
                }, {
                  "cardinality": "38",
                  "category": "General",
                  "name": "model",
                  "longName": "model",
                  "uri": "http://localhost:9998/dimensions/model"
                }, {
                  "cardinality": "100000",
                  "category": "General",
                  "name": "other",
                  "longName": "other",
                  "uri": "http://localhost:9998/dimensions/other"
                }
              ],
              "metrics": [
                {
                  "category": "General",
                  "name":"rowNum",
                  "longName": "rowNum",
                  "uri": "http://localhost:9998/metrics/rowNum"
                }, {
                  "category": "General",
                  "name": "height",
                  "longName": "height",
                  "uri": "http://localhost:9998/metrics/height"
                }, {
                  "category": "General",
                  "name": "width",
                  "longName": "width",
                  "uri": "http://localhost:9998/metrics/width"
                }, {
                  "category": "General",
                  "name": "depth",
                  "longName": "depth",
                  "uri": "http://localhost:9998/metrics/depth"
                }, {
                  "category": "General",
                  "name": "area",
                  "longName": "area",
                  "uri": "http://localhost:9998/metrics/area"
                }, {
                  "category": "General",
                  "name": "volume",
                  "longName": "volume",
                  "uri": "http://localhost:9998/metrics/volume"
                }, {
                  "category": "General",
                  "name": "otherUsers",
                  "longName": "otherUsers",
                  "uri": "http://localhost:9998/metrics/otherUsers"
                }, {
                  "category": "General",
                  "name": "users",
                  "longName": "users",
                  "uri": "http://localhost:9998/metrics/users"
                }
              ]
            }"""
    }
}
