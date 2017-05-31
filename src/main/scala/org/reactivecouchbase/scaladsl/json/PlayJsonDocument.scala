package org.reactivecouchbase.scaladsl.json

import com.couchbase.client.java.document.{Document, RawJsonDocument}
import play.api.libs.json._

object PlayJsonDocument {

  def create(id: String): Document[String] = {
    RawJsonDocument.create(id)
  }

  def create[T](id: String, expiry: Int, content: T)(implicit wrt: Writes[T]): Document[String] = {
    RawJsonDocument.create(id, expiry, Json.stringify(Json.toJson(content)))
  }

  implicit class DocumentPimps(val doc: Document[String]) extends AnyVal {
    def jsResult[T: Reads]: JsResult[T] = {
      Json.fromJson(Json.parse(doc.content()))
    }
    def value[T: Reads]: T = {
      jsResult.get
    }
  }

}
