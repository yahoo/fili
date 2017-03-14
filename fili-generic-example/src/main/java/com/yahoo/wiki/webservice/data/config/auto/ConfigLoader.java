// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import java.util.List;

/**
 * Created by kevin on 3/3/2017.
 */
public interface ConfigLoader {
    List<? extends DruidConfig> getTableNames();
}
