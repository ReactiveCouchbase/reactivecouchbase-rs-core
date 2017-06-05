package org.reactivecouchbase.rs.scaladsl

import play.api.libs.json._

package object json {

  val defaultReads: Reads[JsValue] = Reads.apply(jsv => JsSuccess(jsv))
  val defaultWrites: Writes[JsValue] = Writes.apply(jsv => jsv)

  implicit val defaultFormat: Format[JsValue] = Format(defaultReads, defaultWrites)
}
