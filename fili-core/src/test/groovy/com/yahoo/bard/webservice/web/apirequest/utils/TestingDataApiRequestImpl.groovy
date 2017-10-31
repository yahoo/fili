// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.utils

import static javax.ws.rs.core.Response.Status.OK

import com.yahoo.bard.webservice.data.filterbuilders.DefaultDruidFilterBuilder
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestImpl

import javax.ws.rs.core.Response

class TestingDataApiRequestImpl extends DataApiRequestImpl {
    TestingDataApiRequestImpl() {
        super(
                null,
                null,
                null,
                Response.status(OK),
                null,
                DefaultTimeGrain.DAY,
                [] as Set,
                null,
                [] as Set,
                [] as Set,
                [:],
                null,
                null,
                null,
                0,
                0,
                Long.MAX_VALUE,
                null,
                new DefaultDruidFilterBuilder(),
                null,
                null
        )
    }
}
