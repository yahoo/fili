package com.yahoo.bard.webservice.web.apirequest.rfc;

import com.yahoo.bard.webservice.web.util.BardConfigResources;

public interface Generator<T> {

    T bind(DataApiRequestBuilder builder, RequestParameters params, BardConfigResources resource);

    void validate(T entity, DataApiRequestBuilder builder, RequestParameters params, BardConfigResources resources);
}
