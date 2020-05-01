// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;

/**
 * Contract for building components of a {@link DataApiRequest} with a constructor pattern. Each
 * generator has a bind and validate step, where the resource is generated and then validated being added to the
 * in progress api request.
 *
 * <p>To modify how a data api request is built, you must implement this interface for the type of resource you want
 * to generate. In your {@link AbstractBinderFactory} implementation you must make a named binding for the type of
 * resource you wish to generate. You can only bind one generator per resource (though different resources may share
 * a type, such as "count" and "topN"), so your implementation will replace the default generator for that resource.
 *
 *
 * @param <T> The type of the resource this generator builds.
 */
public interface LegacyGenerator<T> {

    /**
     * Generates the resource.
     *
     * TODO: document standard exceptions this CAN throw (not necessarily caught exceptions)
     *
     * @param request  The request object representing the in progress {@link DataApiRequest}. Previously constructed
     *                 resources are available through this object.
     * @param parameter  The request parameter sent by the client.
     * @param dictionaries  Dictionaries useful in building resources
     *
     * @return the generated resource
     */
    T bind(DataApiRequest request, String parameter, ResourceDictionaries dictionaries);

    /**
     * Validates the generated resource (which means it runs AFTER the {@code bind} method. This is intended to check
     * that the client request is formatted correctly. For example, ensuring that the client requested a positive page
     * number. This method is intended to help reduce parameter checking in the building section.
     *
     * TODO: document standard exceptions this CAN throw (not necessarily caught exceptions)
     *
     * @param entity  The resource constructed by the {@code bind}} method
     * @param request  The request object representing the in progress DataApiRequest
     * @param parameter  The request parameter sent by the client
     * @param dictionaries  Dictionaries useful in building resources
     */
    void validate(T entity, DataApiRequest request, String parameter, ResourceDictionaries dictionaries);
}
