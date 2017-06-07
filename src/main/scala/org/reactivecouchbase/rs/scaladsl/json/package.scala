package org.reactivecouchbase.rs.scaladsl

import akka.util.ByteString
import play.api.libs.json._

package object json {

  //val defaultReads: Reads[JsValue] = Reads.apply(jsv => JsSuccess(jsv))
  //val defaultWrites: Writes[JsValue] = Writes.apply(jsv => jsv)
  //implicit val defaultFormat: Format[JsValue] = Format(defaultReads, defaultWrites)

  val defaultPlayJsonReads: JsonReads[JsValue] = JsonReads(bs => JsonSuccess(Json.parse(bs.utf8String)))
  val defaultPlayJsonWrites: JsonWrites[JsValue] = JsonWrites(jsv => ByteString(Json.stringify(jsv)))

  implicit val defaultPlayJsonFormat: JsonFormat[JsValue] = JsonFormat(defaultPlayJsonReads, defaultPlayJsonWrites)

  // val defaultByteStringReads: JsonReads[ByteString] = JsonReads(bs => JsonSuccess(bs))
  // val defaultByteStringWrites: JsonWrites[ByteString] = JsonWrites(jsv => jsv)
  // implicit val defaultByteStringFormat: JsonFormat[ByteString] = JsonFormat(defaultByteStringReads, defaultByteStringWrites)
}
