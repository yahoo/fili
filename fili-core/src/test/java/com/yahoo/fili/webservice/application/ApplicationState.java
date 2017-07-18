// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.application;

import com.yahoo.fili.webservice.data.cache.DataCache;
import com.yahoo.fili.webservice.druid.client.DruidWebService;
import com.yahoo.fili.webservice.metadata.QuerySigningService;

/**
* Holds the application state when testing so that it can be more easily shared between the JTB and the TBF.
*/
public class ApplicationState {
    DruidWebService uiWebService;
    DruidWebService nonUiWebService;
    DruidWebService metadataWebService;
    DataCache<?> cache;
    QuerySigningService<?> querySigningService;
}
