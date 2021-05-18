// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.requestParameters;
import java.util.Map;

/**
 * Interface describing DataApiRequest arguments.
 */
public interface DataApiRequestParameters extends Map<String, Object> {

    String TABLE = "__table";
    String GRAIN = "__grain";
    String REQUEST_COLUMNS = "__requestColumns";
}
