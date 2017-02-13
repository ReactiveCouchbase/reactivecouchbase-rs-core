package org.reactivecouchbase.scaladsl

import java.util.function.Function

import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import play.api.libs.json._
import rx.functions.{Action0, Action1, Func1}

object RxUtils {
  def func1[T, R](f: T => R): Func1[T, R] = new Func1[T, R]() {
    override def call(t: T): R = f(t)
  }
  def action1[T](f: T => _): Action1[T] = new Action1[T] {
    override def call(t: T): Unit = f(t)
  }
  def action0[T](f: () => _): Action0 = new Action0 {
    override def call(): Unit = f()
  }
}

object JavaUtils {
  def function[T, R](f: T => R): java.util.function.Function[T, R] = {
    new Function[T, R] {
      override def apply(t: T): R = f(t)
    }
  }
}

object JsonConverter {

  import collection.JavaConversions._

  // TODO : fix perfs
  def convertToJson(value: JsObject) = JsonObject.fromJson(Json.stringify(value))

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