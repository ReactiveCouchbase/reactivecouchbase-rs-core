package org.reactivecouchbase.rs.scaladsl.json

import com.couchbase.client.java.document.json.{JsonArray, JsonNull, JsonObject}
import play.api.libs.json._

private[json] object JsonConverter {

  import collection.JavaConversions._

  private def convertJsonValue(value: JsValue): Any = value match {
    case JsNull => JsonNull.INSTANCE
    case JsString(s) => s
    case JsBoolean(b) => b
    case JsNumber(n) => n
    case JsArray(values) => values.foldLeft(JsonArray.create())((a, b) => a.add(convertJsonValue(b)))
    case JsObject(values) => values.toSeq.foldLeft(JsonObject.create())((a, b) => a.put(b._1, convertJsonValue(b._2)))
  }

  def convertToJson(value: JsObject): JsonObject = value.value.toSeq.foldLeft(JsonObject.create())((a, b) => a.put(b._1, convertJsonValue(b._2)))

  def convertToJsValue(value: Any): JsValue = value match {
    case a: JsonObject => JsObject(a.toMap.toMap.mapValues(convertToJsValue))
    case a: JsonArray => JsArray(a.toList.toIndexedSeq.map(convertToJsValue))
    case a: Boolean => JsBoolean(a)
    case a: Double => JsNumber(a)
    case a: Long => JsNumber(a)
    case a: Int => JsNumber(a)
    case a: String => JsString(a)
    case null => JsNull
    case _ => throw new RuntimeException("Unknown type")
  }

  def safeConversion(json: AnyRef): JsValue = {
    json match {
      case s: String => Json.parse(s)
      case a => convertToJsValue(a)
    }
  }
}