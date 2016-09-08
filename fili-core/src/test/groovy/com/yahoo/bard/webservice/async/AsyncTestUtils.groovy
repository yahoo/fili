package com.yahoo.bard.webservice.async

import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.DATE_CREATED
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.DATE_UPDATED
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.JOB_TICKET
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.QUERY
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.STATUS
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.USER_ID

import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSlurper

import org.joda.time.DateTime

import javax.ws.rs.core.UriBuilder

/**
 * Contains a collection of functions to aid in testing asynchronous
 * queries. Particularly around parsing asynchronous payloads and extracting
 * from them the information needed by subsequent requests.
 */
class AsyncTestUtils {

   static final JsonSlurper JSON_PARSER = new JsonSlurper()

   /**
    * Given a JSON representation of the job metadata sent to the user, and a job payload field, returns the value
    * at that field in the specified job metadata.
    *
    * @param response  The job information to extract the field from
    * @param field  The field whose data is of interest
    *
    * @return The value of the specified field in the specified job payload
    */
   static String getJobFieldValue(String response, String field) {
      JSON_PARSER.parseText(response)[field]
   }

   /**
    * Given a JSON representation of the job information sent to the user, returns the URI path for a point lookup of
    * that job's metadata.
    *
    * @param response  The job information to extract the ticket from
    *
    * @return The target to do a point lookup of that job
    */
   static String buildTicketLookup(String response) {
        "jobs/${getJobFieldValue(response, JOB_TICKET.name)}"
   }

   /**
    * Validates that the passed in asynchronous payload has all of the appropriate fields with reasonably correct
    * values.
    *
    * @param asynchronousPayload  The payload to verify
    * @param query  The query that triggered the job
    * @param status  The job's expected status
    */
   static void validateJobPayload(String asynchronousPayload, String query, String status) {
      Map payloadJson = JSON_PARSER.parseText(asynchronousPayload)
      //The test payload builder always sets the user id to greg in TestBinderFactory::buildJobRowBuilder.
      assert payloadJson[USER_ID.name] == "greg"
      assert payloadJson[JOB_TICKET.name].startsWith(payloadJson[USER_ID.name])
      assert payloadJson[STATUS.name] == status
      assert GroovyTestUtils.compareURL(payloadJson[QUERY.name] as String, query)
      assert GroovyTestUtils.compareURL(
              payloadJson["results"],
              "http://localhost:9998/jobs/${payloadJson[JOB_TICKET.name]}/results"
      )
      assert GroovyTestUtils.compareURL(
              payloadJson["syncResults"],
              "http://localhost:9998/jobs/${payloadJson[JOB_TICKET.name]}/results&asyncAfter=never"
      );
      //Validate that the dates are valid dates
      DateTime.parse(payloadJson[DATE_CREATED.name])
      DateTime.parse(payloadJson[DATE_UPDATED.name])
   }

   /**
    * Returns the target (i.e. path) of an HttpRequest.
    *
    * @param httpRequest  The request to turn into a target
    *
    * @return The path component of the request
    */
   static String extractTarget(String httpRequest) {
       new URI(httpRequest).path
   }

    /**
     * Returns the target of the link stored in the specified field of the specified asynchronous payload.
     *
     * @param jobMetadata  The JSON String from which to extract the results link
     * @param jobField  The JSON field in the jobMetadata containing the link
     */
   static String extractTargetFromField(String jobMetadata, String jobField) {
      extractTarget(getJobFieldValue(jobMetadata, jobField))
   }

   /**
    * Given a URI representing a query, extracts the query parameters as a map of Strings (the parameter names) to
    * lists of Strings (the parameter values).
    *
    * @param query  The query whose parameters should be extracted
    *
    * @return The map of query parameters
    */
   static Map<String, List<String>> extractQueryParameters(URI query) {
      query.query.split("&").collectEntries { parameter ->
         def (String key, String value) = parameter.tokenize("=")
         return [(key): value.tokenize(",")]
      }
   }
}
