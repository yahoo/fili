// Copyright 2019 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
import com.yahoo.bard.webservice.sql.presto.PrestoSqlBackedClient


import spock.lang.Specification

class PrestoSqlBackedClientSpec extends Specification {
    def "Test valid sqlQueryToPrestoQuery call"() {
        setup:
        String toPrestoQuery = PrestoSqlBackedClient.sqlQueryToPrestoQuery(sqlQuery)
        expect:
        toPrestoQuery == """SELECT "source", SUBSTRING(datestamp,1,4) AS "\$f23", DAY_OF_YEAR(date_parse(SUBSTRING (datestamp,1,10),'%Y%m%d%H')) AS "\$f24", SUBSTRING(datestamp,9,2) AS "\$f25", SUM("revenue") AS "revenue"
FROM "catalog"."schema"."table"
WHERE "datestamp" > '201909011400000' AND "datestamp" < '201909021400000' AND CAST("comment" AS varchar) <> '1=2====3' AND (CAST("advertiser_id" AS varchar) = '456' OR CAST("advertiser_id" AS varchar) = '123')
GROUP BY "source", SUBSTRING(datestamp,1,4), DAY_OF_YEAR(date_parse(SUBSTRING (datestamp,1,10),'%Y%m%d%H')), SUBSTRING(datestamp,9,2)
ORDER BY SUBSTRING(datestamp,1,4) NULLS FIRST, DAY_OF_YEAR(date_parse(SUBSTRING (datestamp,1,10),'%Y%m%d%H')) NULLS FIRST, SUBSTRING(datestamp,9,2) NULLS FIRST, "source" NULLS FIRST"""

        where:
        sqlQuery << ["""SELECT "source", YEAR("datestamp") AS "\$f23", DAYOFYEAR("datestamp") AS "\$f24", HOUR("datestamp") AS "\$f25", SUM("revenue") AS "revenue"
FROM "catalog"."schema"."table"
WHERE "datestamp" > '201909011400000' AND "datestamp" < '201909021400000' AND CAST("comment" AS varchar) <> '1=2====3' AND (CAST("advertiser_id" AS varchar) = '456' OR CAST("advertiser_id" AS varchar) = '123')
GROUP BY "source", YEAR("datestamp"), DAYOFYEAR("datestamp"), HOUR("datestamp")
ORDER BY YEAR("datestamp") NULLS FIRST, DAYOFYEAR("datestamp") NULLS FIRST, HOUR("datestamp") NULLS FIRST, "source" NULLS FIRST"""]
    }

    def "Test invalid sqlQueryToPrestoQuery call, null input sql query"() {
        when:
        PrestoSqlBackedClient.sqlQueryToPrestoQuery(null)
        then:
        thrown IllegalStateException
    }

    def "Test invalid sqlQueryToPrestoQuery call, empty input sql query"() {
        when:
        PrestoSqlBackedClient.sqlQueryToPrestoQuery("")
        then:
        thrown IllegalStateException
    }

    def "Test invalid sqlQueryToPrestoQuery call, missing timestamp info"() {
        when:
        PrestoSqlBackedClient.sqlQueryToPrestoQuery("SELECT \"source\"")
        then:
        thrown IllegalStateException
    }
}
