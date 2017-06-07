package com.yahoo.bard.webservice;

import com.yahoo.bard.webservice.druid.model.query.DruidQuery;

import org.codehaus.jackson.JsonNode;

import java.util.concurrent.Future;

/**
 * Created by hinterlong on 6/7/17.
 */
public interface SqlBackedClient {
    //todo
    Future<JsonNode> executeQuery(DruidQuery<?> druidQuery);
}
