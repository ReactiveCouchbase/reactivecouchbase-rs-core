package org.reactivecouchbase.rs.scaladsl

import akka.util.ByteString
import io.circe._
import io.circe.syntax._
import io.circe.parser._
import com.couchbase.client.java.document.json._
import org.reactivecouchbase.rs.scaladsl.json._

import scala.language.implicitConversions

package object circejson {

  private def handleParseResult[T](result: Either[io.circe.Error, T]): JsonResult[T] = result.fold[JsonResult[T]](
    failure => JsonError(List(JsonValidationError(List(failure.getMessage)))),
    success => JsonSuccess(success)
  )

  private val printer: Printer = Printer.noSpaces.copy(dropNullValues = true)

  val defaultCirceReads: JsonReads[Json]   = JsonReads(bs => handleParseResult(parse(bs.utf8String)))
  val defaultCirceWrites: JsonWrites[Json] = JsonWrites(jsv => ByteString(printer.pretty(jsv)))

  implicit val defaultCirceFormat: JsonFormat[Json] = JsonFormat(defaultCirceReads, defaultCirceWrites)

  /**
    * Converts between io.circe.Json and org.reactivecouchbase.rs.scaladsl.json objects
    * @param encoder [[https://circe.github.io/circe/codec.html#custom-encodersdecoders Encoder]] for ModelType
    * @param decoder [[https://circe.github.io/circe/codec.html#custom-encodersdecoders Decoder]] for ModelType
    * @return JsonFormat[MODELTYPE]
    * */
  def createCBFormat[MODELTYPE](implicit encoder: Encoder[MODELTYPE],
                                decoder: Decoder[MODELTYPE]): JsonFormat[MODELTYPE] =
    JsonFormat[MODELTYPE](
      JsonReads[MODELTYPE](bs => handleParseResult(decode[MODELTYPE](bs.utf8String))),
      JsonWrites[MODELTYPE](jsv => ByteString(printer.pretty(jsv.asJson)))
    )

  implicit val defaultCirceConverter: CouchbaseJsonDocConverter[Json] = new CouchbaseJsonDocConverter[Json] {
    override def convertTo(ref: AnyRef): Json = convertToJson(ref)
    override def convertFrom(ref: Json): Any  = convertJsonValue(ref)
  }

  case class CirceQueryParams(query: Json) extends QueryParams {
    override def isEmpty: Boolean                                                 = query.isNull
    override def toJsonObject: com.couchbase.client.java.document.json.JsonObject = convertToJson(query)
  }

  implicit class EnhancedJsonObject(val obj: Json) extends AnyVal {
    def asQueryParams: CirceQueryParams = CirceQueryParams(obj)
  }

  import collection.JavaConverters._

  private def convertJsonValue(value: Json): Any = value match {
    case x if x.isNull    => JsonNull.INSTANCE
    case x if x.isBoolean => x.asBoolean.getOrElse(JsonNull.INSTANCE)
    case x if x.isString  => x.asString.getOrElse(JsonNull.INSTANCE)
    case x if x.isNumber  => x.asNumber.flatMap(_.toBigDecimal.map(_.bigDecimal)).getOrElse(JsonNull.INSTANCE)
    case x if x.isArray =>
      x.asArray
        .map(_.foldLeft(JsonArray.create())((a, b) => a.add(convertJsonValue(b))))
        .getOrElse(JsonNull.INSTANCE)
    case x if x.isObject =>
      x.asObject
        .map(
          _.toList.foldLeft(com.couchbase.client.java.document.json.JsonObject.create())(
            (a, b) => a.put(b._1, convertJsonValue(b._2))
          )
        )
        .getOrElse(JsonNull.INSTANCE)
    case _ => throw new RuntimeException("Unknown type")
  }

  private def convertToJson(value: Json): com.couchbase.client.java.document.json.JsonObject =
    value.asObject
      .map { x: io.circe.JsonObject =>
        x.toList.foldLeft(com.couchbase.client.java.document.json.JsonObject.create())(
          (a, b) => a.put(b._1, convertJsonValue(b._2))
        )
      }
      .getOrElse(com.couchbase.client.java.document.json.JsonObject.empty())

  private def convertToJson(value: Any): Json = value match {
    case a: com.couchbase.client.java.document.json.JsonObject =>
      Json.obj(a.toMap.asScala.toMap.mapValues(convertToJson).toList: _*)
    case a: JsonArray => Json.arr(a.toList.asScala.toIndexedSeq.map(convertToJson): _*)
    case a: Boolean   => Json.fromBoolean(a)
    case a: Double    => Json.fromDouble(a).getOrElse(Json.Null)
    case a: Long      => Json.fromLong(a)
    case a: Int       => Json.fromInt(a)
    case a: String    => Json.fromString(a)
    //noinspection ScalaStyle
    case null => Json.Null
    case _    => throw new RuntimeException("Unknown type")
  }

}
