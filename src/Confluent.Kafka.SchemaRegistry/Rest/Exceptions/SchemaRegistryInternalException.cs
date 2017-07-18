using System;
using System.Net;

namespace  Confluent.Kafka.SchemaRegistry.Rest.Exceptions
{
    /// <summary>
    /// All API endpoints use a standard error message format for any requests that return an HTTP status indicating an error (any 400 or 500 statuses). For example, a request entity that omits a required field may generate the following response:
    /// </summary>
    public class SchemaRegistryInternalException : Exception
    {
        /// <summary>
        /// ErrorCode specfic to schemaSerializer, of form XXX or XXXYY 
        /// where XXX is standard http error status (400-500) and YY specific to schema registry
        /// Example: 40403 = Schema not found
        /// </summary>
        public int ErrorCode { get; }

        /// <summary>
        /// Standard Response of html request
        /// </summary>
        public HttpStatusCode Status { get; }

        public SchemaRegistryInternalException(string message, HttpStatusCode status, int errorCode) : base(message + "; error code: " + errorCode)
        {
            ErrorCode = errorCode;
            Status = status;
        }
    }
}