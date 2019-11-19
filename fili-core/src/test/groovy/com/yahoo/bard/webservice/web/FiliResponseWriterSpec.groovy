// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.util.GroovyTestUtils

class FiliResponseWriterSpec extends ResponseWriterSpec {

    private final FiliResponseWriter filiResponseWriter = new FiliResponseWriter(new FiliResponseWriterSelector(
            new CsvResponseWriter(MAPPERS),
            new JsonResponseWriter(MAPPERS),
            new JsonApiResponseWriter(MAPPERS)
    ))

    def "test FiliResponseWriter on Json format"() {
        given:
        formattedDateTime = dateTime.toString(getDefaultFormat())

        and: "The expected serialization result from writer"
        String expectedJson ="""{
              "rows": [
                {
                  "dateTime": "${-> formattedDateTime}",
                  "product|id": "ymail",
                  "product|desc": "yahoo, mail",
                  "platform|id": "mob",
                  "platform|desc": "mobile \\" desc..",
                  "property|desc": "United States",
                  "pageViews": 10,
                  "timeSpent": 10
                },
                {
                  "dateTime": "${-> formattedDateTime}",
                  "product|id": "ysports",
                  "product|desc": "yahoo sports",
                  "platform|id": "desk",
                  "platform|desc": "desktop ,\\" desc..",
                  "property|desc": "India",
                  "pageViews": 10,
                  "timeSpent": 10
                }
              ]
            }"""

        and: "The format type for serialization"
        apiRequest.getFormat() >> DefaultResponseFormatType.JSON

        when: "Serialize the data with FiliResponseWriter"
        filiResponseWriter.write(apiRequest,response,os)

        then: "The serialization is correct"
        GroovyTestUtils.compareJson(os.toString(), expectedJson)
    }

    def "test FiliResponseWriter on Csv format"() {
        given:
        formattedDateTime = dateTime.toString(getDefaultFormat())

        and: "The expected serialization result from writer"
        String expectedCSV =
                """dateTime,product|id,product|desc,platform|id,platform|desc,property|desc,pageViews,timeSpent
"${-> formattedDateTime}",ymail,"yahoo, mail",mob,"mobile "" desc..","United States",10,10
"${-> formattedDateTime}",ysports,"yahoo sports",desk,"desktop ,"" desc..",India,10,10
"""

        and: "The format type for serialization"
        apiRequest.getFormat() >> DefaultResponseFormatType.CSV

        when: "Serialize the data with FiliResponseWriter "
        filiResponseWriter.write(apiRequest,response,os)

        then: "The serialization is correct"
        os.toString() == expectedCSV
    }

    def "test FiliResponseWriter on JsonApi format"() {
        given:
        formattedDateTime = dateTime.toString(getDefaultFormat())

        and: "The expected serialization result from writer"
        String expectedJsonApi ="""{
          "rows": [
            {
              "dateTime": "${-> formattedDateTime}",
              "product": "ymail",
              "platform": "mob",
              "property": "US",
              "pageViews": 10,
              "timeSpent": 10
            },
            {
              "dateTime": "${-> formattedDateTime}",
              "product": "ysports",
              "platform": "desk",
              "property": "IN",
              "pageViews": 10,
              "timeSpent": 10
            }
          ],
          "platform": [
            {
              "id": "mob",
              "desc": "mobile \\" desc.."
            },
            {
              "id": "desk",
              "desc": "desktop ,\\" desc.."
            }
          ],
          "property": [
            {
              "desc": "United States",
              "id": "US"
            },
            {
              "desc": "India",
              "id": "IN"
            }
          ],
          "product": [
            {
              "id": "ymail",
              "desc": "yahoo, mail"
            },
            {
              "id": "ysports",
              "desc": "yahoo sports"
            }
          ]
        }"""

        and: "The format type for serialization"
        apiRequest.getFormat() >> DefaultResponseFormatType.JSONAPI

        when: "Serialize the data with FiliResponseWriter "
        FiliResponseWriter filiResponseWriter = new FiliResponseWriter(new FiliResponseWriterSelector(
                new CsvResponseWriter(MAPPERS),
                new JsonResponseWriter(MAPPERS),
                new JsonApiResponseWriter(MAPPERS)
        ))
        filiResponseWriter.write(apiRequest,response,os)

        then: "The serialization is correct"
        GroovyTestUtils.compareJson(os.toString(), expectedJsonApi)
    }
}
