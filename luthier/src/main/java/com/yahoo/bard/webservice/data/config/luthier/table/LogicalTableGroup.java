// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.table;

import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.TableIdentifier;

import java.util.LinkedHashMap;

/**
 * Represents the set of LogicalTables that differs by granularity.
 * This is used to build multiple LogicalTables from the config file.
 */
public class LogicalTableGroup extends LinkedHashMap<TableIdentifier, LogicalTable> {
}
