// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import java.util.LinkedHashMap;

import javax.inject.Singleton;

/**
 * Map of a druid table name to PhysicalTable.
 */
@Singleton
public class PhysicalTableDictionary extends LinkedHashMap<String, ConfigPhysicalTable> {
    // Empty Class
}
