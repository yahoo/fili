// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.LogicalDimensionColumn
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.metric.MetricColumnWithValueType
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.druid.model.query.Granularity
import com.yahoo.bard.webservice.table.Schema
import com.yahoo.bard.webservice.table.ZonedSchema
import com.yahoo.bard.webservice.util.SimplifiedIntervalList
import com.yahoo.bard.webservice.web.PreResponse
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval

import spock.lang.Specification
/**
 * PreResponse object serialization and de-serialization tests resource
 */
class SerializationResources extends Specification {

    DimensionDictionary dimensionDictionary
    PreResponse preResponse
    ResultSet resultSet
    Result result1, result2, result3, result4, result5
    ResponseContext responseContext, responseContext1
    Schema schema, schema2, schema3
    HashMap dimensionRows1
    Map<MetricColumn, Object>  metricValues1, metricValues2, metricValues3, metricValues4, metricValues5
    Granularity granularity
    Interval interval
    BigDecimal bigDecimal

    SerializationResources init() {
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        def dimensionNames = ["ageBracket", "gender", "country"]

        dimensionDictionary = new DimensionDictionary()
        KeyValueStoreDimension dimension
        for (String name : dimensionNames) {
            dimension = new KeyValueStoreDimension(
                    name,
                    name,
                    dimensionFields,
                    MapStoreManager.getInstance("dimension"),
                    ScanSearchProviderManager.getInstance(name)
            )
            dimension.setLastUpdated(new DateTime(10000))
            dimensionDictionary.add(dimension)
        }

        dimensionRows1 = new HashMap<>()
        HashMap dimensionRows2 = new HashMap<>()

        //ageBracket dimension rows preparation
        Dimension ageBracketDim =  dimensionDictionary.findByApiName("ageBracket")

        DimensionRow ageBracketRow1 = BardDimensionField.makeDimensionRow(ageBracketDim, "1", "1")
        dimensionRows1.put(new LogicalDimensionColumn(ageBracketDim), ageBracketRow1)
        ageBracketDim.addDimensionRow(ageBracketRow1)

        DimensionRow ageBracketRow2 = BardDimensionField.makeDimensionRow(ageBracketDim, "4", "4")
        dimensionRows2.put(new LogicalDimensionColumn(ageBracketDim), ageBracketRow2)
        ageBracketDim.addDimensionRow(ageBracketRow2)


        //Gender dimension preparation
        Dimension genderDim = dimensionDictionary.findByApiName("gender")

        DimensionRow gendeRow1 = BardDimensionField.makeDimensionRow(genderDim, "m", "m")
        dimensionRows1.put(new LogicalDimensionColumn(genderDim), gendeRow1)
        genderDim.addDimensionRow(gendeRow1)

        DimensionRow gendeRow2 = BardDimensionField.makeDimensionRow(genderDim, "f", "f")
        dimensionRows2.put(new LogicalDimensionColumn(genderDim), gendeRow2)
        genderDim.addDimensionRow(gendeRow2)


        //Country dimension preparation
        Dimension countryDim = dimensionDictionary.findByApiName("country")

        DimensionRow countyRow1 = BardDimensionField.makeDimensionRow(countryDim, "US", "US")
        dimensionRows1.put(new LogicalDimensionColumn(countryDim), countyRow1)
        countryDim.addDimensionRow(countyRow1)

        DimensionRow countyRow2 = BardDimensionField.makeDimensionRow(countryDim, "IN", "IN")
        dimensionRows2.put(new LogicalDimensionColumn(countryDim), countyRow2)
        countryDim.addDimensionRow(countyRow2)

        metricValues1 = new HashMap()
        metricValues1.put(new MetricColumn("simplePageViews"), new BigDecimal(111))
        metricValues1.put(new MetricColumn("lookbackPageViews"), new BigDecimal(112))
        metricValues1.put(new MetricColumn("retentionPageViews"), new BigDecimal(113))

        metricValues2 = new HashMap()
        metricValues2.put(new MetricColumn("simplePageViews"), new BigDecimal(211))
        metricValues2.put(new MetricColumn("lookbackPageViews"), new BigDecimal(212))
        metricValues2.put(new MetricColumn("retentionPageViews"), new BigDecimal(213))

        metricValues3 = new HashMap()
        metricValues3.put(new MetricColumn("simplePageViews"), new BigDecimal(311))
        metricValues3.put(new MetricColumn("lookbackPageViews"), new BigDecimal(312))
        metricValues3.put(new MetricColumn("retentionPageViews"), new BigDecimal(313))
        metricValues3.put(new MetricColumn("rawSketch"), "0101010101")
        metricValues3.put(new MetricColumn("listMetric"), [1, "2", 3])

        metricValues4 = new HashMap()
        metricValues4.put(new MetricColumn("simplePageViews"), new BigDecimal(111))
        metricValues4.put(new MetricColumn("lookbackPageViews"), null)
        metricValues4.put(new MetricColumn("retentionPageViews"), new BigDecimal(113))

        //result preparation
        result1 = new Result(dimensionRows1, metricValues1, DateTime.parse(("2016-01-12T00:00:00.000Z")))
        result2 = new Result(dimensionRows2, metricValues2, DateTime.parse(("2016-01-12T00:00:00.000Z")))
        result3 = new Result(dimensionRows2, metricValues3, DateTime.parse(("2016-01-12T00:00:00.000Z")))

        result4 = new Result(dimensionRows2, metricValues4, DateTime.parse(("2016-01-12T00:00:00.000Z")))

        StandardGranularityParser granularityParser = new StandardGranularityParser()
        granularity = granularityParser.parseGranularity("day", DateTimeZone.UTC);

        Map<String, String> baseSchemaTypeMap = [
                "simplePageViews": "java.math.BigDecimal",
                "lookbackPageViews": "java.math.BigDecimal",
                "retentionPageViews": "java.math.BigDecimal"]

        schema = buildSchema(baseSchemaTypeMap)

        baseSchemaTypeMap.putAll("rawSketch": "java.lang.String", "listMetric": "java.util.List")
        schema3 = buildSchema(baseSchemaTypeMap)

        List<Result> results = new ArrayList<>([result1, result2])
        resultSet = new ResultSet(results, schema)

        DateTime ny = new DateTime(2011, 2, 2, 7, 0, 0, 0, DateTimeZone.forID("UTC"));
        DateTime la = new DateTime(2011, 2, 3, 10, 15, 0, 0, DateTimeZone.forID("UTC"));
        interval = new Interval(ny, la)
        bigDecimal = new BigDecimal("100")

        responseContext = new ResponseContext([:])
        responseContext.put("randomHeader", "someHeader")
        responseContext.put("missingIntervals", ["a","b","c", new SimplifiedIntervalList([interval]), bigDecimal])

        responseContext1 = new ResponseContext([:])
        responseContext1.put("randomHeader", "someHeader")
        responseContext1.put("apiMetricColumnNames", ["metric1, metric2"] as Set)
        responseContext1.put("requestedApiDimensionFields", [(ageBracketDim.getApiName()) : [BardDimensionField.ID] as Set])

        preResponse = new PreResponse(resultSet, responseContext)

        return this
    }

    ZonedSchema buildSchema(Map<String, String> metricNameClassNames) {
        Schema schema = new ZonedSchema(granularity, DateTimeZone.UTC)
        schema.addColumn(new LogicalDimensionColumn(dimensionDictionary.findByApiName("ageBracket")))
        schema.addColumn(new LogicalDimensionColumn(dimensionDictionary.findByApiName("gender")))
        schema.addColumn(new LogicalDimensionColumn(dimensionDictionary.findByApiName("country")))
        metricNameClassNames.each {
             schema.addColumn(new MetricColumnWithValueType(it.key, it.value))
        }
        return schema
    }

    String getSerializedResultSet(){
        """
            {
                "results": [
                  {
                    "dimensionValues": {
                      "ageBracket": "1",
                      "country": "US",
                      "gender": "m"
                    },
                    "metricValues": {
                      "lookbackPageViews": 112,
                      "retentionPageViews": 113,
                      "simplePageViews": 111
                    },
                    "timeStamp": "2016-01-12T00:00:00.000Z"
                  },
                  {
                    "dimensionValues": {
                      "ageBracket": "4",
                      "country": "IN",
                      "gender": "f"
                    },
                    "metricValues": {
                      "lookbackPageViews": 212,
                      "retentionPageViews": 213,
                      "simplePageViews": 211
                    },
                    "timeStamp": "2016-01-12T00:00:00.000Z"
                  }
                ],
                "schema": {
                  "dimensionColumns": [
                    "ageBracket",
                    "country",
                    "gender"
                  ],
                  "granularity": "day",
                  "metricColumns": [
                    "lookbackPageViews",
                    "simplePageViews",
                    "retentionPageViews"
                  ],
                   "metricColumnsType": {
                     "lookbackPageViews": "java.math.BigDecimal",
                      "retentionPageViews": "java.math.BigDecimal",
                      "simplePageViews": "java.math.BigDecimal"
                  },
                  "timeZone": "UTC"
                }
            }
        """
    }

    String getSerializedResult2(){
        """
            {
              "dimensionValues": {
                "ageBracket": "4",
                "country": "IN",
                "gender": "f"
              },
              "metricValues": {
                "simplePageViews": 211,
                "lookbackPageViews": 212,
                "retentionPageViews": 213
              },
              "timeStamp": "2016-01-12T00:00:00.000Z"
            }
        """
    }

    String getSerializedResult3(){
       """
          {
              "dimensionValues": {
                "ageBracket": "4",
                "country": "IN",
                "gender": "f"
              },
              "metricValues": {
                "simplePageViews": 311,
                "lookbackPageViews": 312,
                "retentionPageViews": 313,
                "rawSketch": "0101010101",
                "listMetric": [1, "2",3]
              },
              "timeStamp": "2016-01-12T00:00:00.000Z"
          }
        """
    }

    String getSerializedPreResponse(){
        """
            {
              "responseContext": "[\\"com.yahoo.bard.webservice.web.responseprocessors.ResponseContext\\",{\\"randomHeader\\":\\"someHeader\\",\\"missingIntervals\\":[\\"java.util.ArrayList\\",[\\"a\\",\\"b\\",\\"c\\",[\\"java.util.ArrayList\\",[[\\"org.joda.time.Interval\\",\\"2011-02-02T07:00:00.000Z/2011-02-03T10:15:00.000Z\\"]]],[\\"java.math.BigDecimal\\",100]]]}]",
              "resultSet": {
                "results": [
                  {
                    "dimensionValues": {
                      "ageBracket": "1",
                      "country": "US",
                      "gender": "m"
                    },
                    "metricValues": {
                      "lookbackPageViews": 112,
                      "retentionPageViews": 113,
                      "simplePageViews": 111
                    },
                    "timeStamp": "2016-01-12T00:00:00.000Z"
                  },
                  {
                    "dimensionValues": {
                      "ageBracket": "4",
                      "country": "IN",
                      "gender": "f"
                    },
                    "metricValues": {
                      "lookbackPageViews": 212,
                      "retentionPageViews": 213,
                      "simplePageViews": 211
                    },
                    "timeStamp": "2016-01-12T00:00:00.000Z"
                  }
                ],
                "schema": {
                  "dimensionColumns": [
                    "ageBracket",
                    "country",
                    "gender"
                  ],
                  "granularity": "day",
                  "metricColumns": [
                    "lookbackPageViews",
                    "retentionPageViews",
                    "simplePageViews"
                  ],
                  "metricColumnsType": {
                    "lookbackPageViews": "java.math.BigDecimal",
                    "retentionPageViews": "java.math.BigDecimal",
                    "simplePageViews": "java.math.BigDecimal"
                  },
                  "timeZone": "UTC"
                }
              }
            }
        """
    }

    String getSerializedResponseContext(){
        """
        {
          "responseContext": "[\\"com.yahoo.bard.webservice.web.responseprocessors.ResponseContext\\",{\\"randomHeader\\":\\"someHeader\\",\\"missingIntervals\\":[\\"java.util.ArrayList\\",[\\"a\\",\\"b\\",\\"c\\",[\\"java.util.ArrayList\\",[[\\"org.joda.time.Interval\\",\\"2011-02-02T07:00:00.000Z/2011-02-03T10:15:00.000Z\\"]]],[\\"java.math.BigDecimal\\",100]]]}]"
        }
        """
    }

    String getSerializedReponseContext1() {
        """["com.yahoo.bard.webservice.web.responseprocessors.ResponseContext",{"randomHeader":"someHeader","apiMetricColumnNames":["java.util.LinkedHashSet",["metric1, metric2"]],"requestedApiDimensionFields":["java.util.LinkedHashMap",{"ageBracket":["java.util.LinkedHashSet",[["com.yahoo.bard.webservice.data.dimension.BardDimensionField","ID"]]]}]}]"""
    }
}
