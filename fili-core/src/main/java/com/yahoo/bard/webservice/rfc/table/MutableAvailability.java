// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.rfc.table;

import com.yahoo.bard.webservice.table.Column;

import org.joda.time.Interval;

import java.util.HashMap;
import java.util.List;

public class MutableAvailability extends HashMap<Column, List<Interval>> implements Availability {
}
