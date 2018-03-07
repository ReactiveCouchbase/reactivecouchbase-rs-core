package org.reactivecouchbase.rs.scaladsl

import akka.util.ByteString
import com.couchbase.client.java.document.json._
import play.api.libs.json._
import scala.language.implicitConversions

package object json {

  val defaultPlayJsonReads: JsonReads[JsValue]   = JsonReads(bs => JsonSuccess(Json.parse(bs.utf8String)))
  val defaultPlayJsonWrites: JsonWrites[JsValue] = JsonWrites(jsv => ByteString(Json.stringify(jsv)))

  implicit val defaultPlayJsonFormat: JsonFormat[JsValue] = JsonFormat(defaultPlayJsonReads, defaultPlayJsonWrites)

  /**
   * Converts between play.api.libs.json and org.reactivecouchbase.rs.scaladsl.json objects
   * @param modelFormat Implicit play.api.libs.json.OFormat object for the MODELTYPE to serialize
   * @return JsonFormat[MODELTYPE]
   * */
  implicit def convertJsonOFormat[MODELTYPE](modelFormat: OFormat[MODELTYPE]): JsonFormat[MODELTYPE] =
    JsonFormat[MODELTYPE](
      JsonReads[MODELTYPE](
        bs =>
          modelFormat
            .reads(Json.parse(bs.utf8String))
            .map(result => JsonSuccess(result))
            .getOrElse[JsonResult[MODELTYPE]](JsonError())
      ),
      JsonWrites[MODELTYPE](jsv => ByteString(Json.stringify(modelFormat.writes(jsv))))
    )

  /**
   * Converts between play.api.libs.json and org.reactivecouchbase.rs.scaladsl.json objects
   * @param modelFormat Implicit play.api.libs.json.Format object for the MODELTYPE to serialize
   * @return JsonFormat[MODELTYPE]
   * */
  implicit def convertJsonFormat[MODELTYPE](modelFormat: Format[MODELTYPE]): JsonFormat[MODELTYPE] =
    JsonFormat[MODELTYPE](
      JsonReads[MODELTYPE](
        bs =>
          modelFormat
            .reads(Json.parse(bs.utf8String))
            .map(result => JsonSuccess(result))
            .getOrElse[JsonResult[MODELTYPE]](JsonError())
      ),
      JsonWrites[MODELTYPE](jsv => ByteString(Json.stringify(modelFormat.writes(jsv))))
    )

  implicit val defaultPlayJsonConverter: CouchbaseJsonDocConverter[JsValue] = new CouchbaseJsonDocConverter[JsValue] {
    override def convertTo(ref: AnyRef): JsValue = JsonConverter.convertToJsValue(ref)
    override def convertFrom(ref: JsValue): Any  = JsonConverter.convertJsonValue(ref)
  }

  case class PlayJsonQueryParams(query: JsObject = Json.obj()) extends QueryParams {
    override def isEmpty: Boolean         = query.value.isEmpty
    override def toJsonObject: JsonObject = JsonConverter.convertToJson(query)
  }

  implicit class EnhancedJsObject(val obj: JsObject) extends AnyVal {
    def asQueryParams: PlayJsonQueryParams = PlayJsonQueryParams(obj)
  }

  implicit class EnhancedJsValue(val value: JsValue) extends AnyVal {
    def asCbValue: Any = defaultPlayJsonConverter.convertFrom(value)
  }

  implicit class EnhancedJsonArray(val value: JsonArray) extends AnyVal {
    def asJsValue: Any = defaultPlayJsonConverter.convertTo(value)
  }

  implicit class EnhancedJsonObject(val value: JsonObject) extends AnyVal {
    def asJsValue: Any = defaultPlayJsonConverter.convertTo(value)
  }
}
