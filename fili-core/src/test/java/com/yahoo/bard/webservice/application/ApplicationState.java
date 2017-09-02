// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.data.cache.DataCache;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.metadata.QuerySigningService;

/**
* Holds the application state when testing so that it can be more easily shared between the JTB and the TBF.
*/
public class ApplicationState {
    DruidWebService webService;
    DruidWebService metadataWebService;
    DataCache<?> cache;
    QuerySigningService<?> querySigningService;
}
