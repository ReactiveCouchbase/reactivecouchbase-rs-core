package org.reactivecouchbase.rs.scaladsl

import akka.util.ByteString
import play.api.libs.json._

package object json {
  val defaultPlayJsonReads: JsonReads[JsValue] = JsonReads(bs => JsonSuccess(Json.parse(bs.utf8String)))
  val defaultPlayJsonWrites: JsonWrites[JsValue] = JsonWrites(jsv => ByteString(Json.stringify(jsv)))
  implicit val defaultPlayJsonFormat: JsonFormat[JsValue] = JsonFormat(defaultPlayJsonReads, defaultPlayJsonWrites)
}
