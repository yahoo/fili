package com.yahoo.bard.webservice.web.apirequest.rfc

import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest

import spock.lang.Specification

class DataApiRequestImplSpec extends Specification {

    def "DateTime sort correctly removed from StandardSorts"() {
        expect:
        DataApiRequestImpl.extractDateTimeSort([
                new OrderByColumn(DataApiRequest.DATE_TIME_STRING, SortDirection.DESC),
                new OrderByColumn("other1", SortDirection.ASC),
                new OrderByColumn("other2", SortDirection.DESC),
        ] as LinkedHashSet).isPresent()

        and:
        !DataApiRequestImpl.extractDateTimeSort([
                new OrderByColumn("other1", SortDirection.ASC),
                new OrderByColumn("other2", SortDirection.DESC),
        ] as LinkedHashSet).isPresent()
    }

    def "standard sorts correctly remove date time sort"() {
        given:
        LinkedHashSet<OrderByColumn> allSorts = [
                new OrderByColumn(DataApiRequest.DATE_TIME_STRING, SortDirection.ASC),
                new OrderByColumn("other1", SortDirection.ASC),
                new OrderByColumn("other2", SortDirection.DESC),
                new OrderByColumn("other3", SortDirection.ASC),
        ] as LinkedHashSet

        expect:
        DataApiRequestImpl.extractStandardSorts(allSorts) == [
                new OrderByColumn("other1", SortDirection.ASC),
                new OrderByColumn("other2", SortDirection.DESC),
                new OrderByColumn("other3", SortDirection.ASC),
        ] as LinkedHashSet
    }
}
