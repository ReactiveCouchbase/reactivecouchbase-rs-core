package org.reactivecouchbase.rs.scaladsl

import akka.util.ByteString
import com.couchbase.client.java.document.json.JsonObject
import play.api.libs.json._

package object json {

  val defaultPlayJsonReads: JsonReads[JsValue] = JsonReads(bs => JsonSuccess(Json.parse(bs.utf8String)))
  val defaultPlayJsonWrites: JsonWrites[JsValue] = JsonWrites(jsv => ByteString(Json.stringify(jsv)))

  implicit val defaultPlayJsonFormat: JsonFormat[JsValue] = JsonFormat(defaultPlayJsonReads, defaultPlayJsonWrites)
  implicit val defaultPlayJsonConverter: CouchbaseJsonDocConverter[JsValue] = (ref: AnyRef) => JsonConverter.convertToJsValue(ref)
  implicit val defaultPlayJsonEmptyQueryParams: () => QueryParams = () => PlayJsonQueryParams()

  case class PlayJsonQueryParams(query: JsObject = Json.obj()) extends QueryParams {
    override def isEmpty: Boolean = query.value.isEmpty
    override def toJsonObject: JsonObject = JsonConverter.convertToJson(query)
  }

  implicit class EnhancedJsonObject(val obj: JsObject) extends AnyVal {
    def asQueryParams: PlayJsonQueryParams = PlayJsonQueryParams(obj)
  }
}
