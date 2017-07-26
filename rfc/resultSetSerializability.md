# Flexible Response Format #

## Overview ##

Currently, Fili's result set serialization is very rigid.
This is a huge pain point whenever a customer needs to tweak response
serialization, or completely customizing how a `ResultSet` is serialized.

This RFC aims to solve this problem by introducing a means of registering
custom response writers. Essentially, customers will be able to define a function
that takes the `DataApiRequest` and returns a `ResponseWriter`. This will allow
them to use any and all information in the request to decide what kind of
`ResultSet` needs to be written and return the appropriate writer.

## Goals ##

Allow customers to define their own response writer that grants them
complete control over how a `ResultSet` is serialized to the user.

Customers will be able to do this by defining a custom implementation of a
SAM interface called `ResponseWriter`, and injecting the selector via the
`BinderFactory`.

## Implementation ##

1. Define an interface `ResponseWriter` with a single method:
    ```java
        void write(DataApiRequest request, ResponseData responseData, Outputstream os);
    ```
    This method takes in a data object containing all the information
    (including the `ResultSet`) needed to write the Fili response, and writes
    the response to the passed in output stream.

2. Split the `Response` class into four pieces:
    a. A `ResponseData` class that contains all of the data and helper methods
        currently living on `Response`.
    b. An implementation of `ResponseWriter` for each of
        `Response::writeJsonResponse`, `Response::writeJsonApiResponse`,
        `Response::writeCsvResponse`.

3. Define an interface `ResponseWriterSelector` with a single method:
    ```java
        ResponseWriter select(DataApiRequest request);
    ```
    This object is responsible for choosing a `ResponseWriter` to use based on
    the `DataApiRequest`. It will be used as part of the default
    implementation of `ResponseWriter`.

4. Write a default implementation of `ResponseWriterSelector` that chooses and
    delegates to one of the classes defined in 4b based on the `ResponseType`
    in `DataApiRequest`.

5. Define a default implementation of `ResponseWriter`, `FiliResponseWriter`
    that uses a
    `ResponseWriterSelector`. This class will also include a method
    ```java
        void addResponseType(ResponseType type, ResponseWriter writer);
    ```
    That allows users to add an additional mapping from a response type to the
    writer. Then, if a customer so desires, they can build a
    `FiliResponseWriter`, and invoke `addResponseType` to register a writer
    with the desired type instead of implementing a
    `ResponseWriter` from scratch that handles every type if they don't need
    to.

5. Make `HttpResponseMaker` injectable. The `HttpResponseMaker` does not have
any response-specific state, so there is no reason not to make it injectable.
In the worst case, this also allows customers to build their own custom
`HttpResponseMaker` and rework _all_ the response making logic.

6. Tweak the `HttpResponseMaker::buildResponse` to also take an
`ApiRequest`. `HttpResponseMaker::buildResponse` is where we will be
deciding which writer is going to be used, so it will need the
`ApiRequest`. This will also require updating the `HttpResponseChannel`.
We need to define a constructor that takes an `ApiRequest`, and
remove the constructor that does not, so that the `HttpResponseChannel` can
pass the `DataApiRequest` to `HttpResponseMaker::buildResponse`.

# Milestones #

1. Make `HttpResponseMaker` injectable, and tweak
    `HttpResponseMaker::buildResponse` to take a `DataApiRequest`. This will
    allow customers to override all the respone building logic. Not ideal, but
    sufficient.

2. Define the `ResponseWriter` and use it.
