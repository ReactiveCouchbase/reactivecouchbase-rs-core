package org.reactivecouchbase.rs.scaladsl.json

import akka.util.ByteString
import com.couchbase.client.java.document.json.JsonObject
import play.api.libs.json.Json

package object bytestring {

  val defaultByteStringReads: JsonReads[ByteString] = JsonReads(bs => JsonSuccess(bs))
  val defaultByteStringWrites: JsonWrites[ByteString] = JsonWrites(bs => bs)

  implicit val defaultByteStringFormat: JsonFormat[ByteString] = JsonFormat(defaultByteStringReads, defaultByteStringWrites)

  implicit val defaultByteStringConverter: CouchbaseJsonDocConverter[ByteString] = new CouchbaseJsonDocConverter[ByteString] {
    override def convertTo(ref: AnyRef): ByteString = ByteString(Json.stringify(JsonConverter.convertToJsValue(ref)))
    override def convertFrom(ref: ByteString): Any = JsonConverter.convertJsonValue(Json.parse(ref.utf8String))
  }

  case class ByteStringQueryParams(query: ByteString = ByteString.empty) extends QueryParams {
    override def isEmpty: Boolean = query.isEmpty
    override def toJsonObject: JsonObject = JsonObject.fromJson(query.utf8String)
  }

  implicit class EnhancedByteString(val obj: ByteString) extends AnyVal {
    def asQueryParams: ByteStringQueryParams = ByteStringQueryParams(obj)
  }
}
