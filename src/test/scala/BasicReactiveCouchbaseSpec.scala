package org.reactivecouchbase.rs.tests

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.view.{DesignDocument, Stale}
import com.typesafe.config.ConfigFactory
import io.circe.{Decoder, Encoder}
import io.circe.syntax._
import io.circe.generic.semiauto._
import org.reactivecouchbase.rs.scaladsl.json._
import org.reactivecouchbase.rs.scaladsl.circejson._
import org.reactivecouchbase.rs.scaladsl.{N1qlQuery, ReactiveCouchbase, ViewQuery}
import org.scalatest._
import play.api.libs.json._

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Try

sealed case class TestModel(message: String, `type`: Option[String])
sealed case class TestModel2(message: String, `type`: Option[String])
sealed case class Person(name: String, surname: String, age: Int)



class BasicReactiveCouchbaseSpec extends FlatSpec with Matchers {

  import TestImplicits._


  def timeout(duration: FiniteDuration)(implicit system: ActorSystem, ec: ExecutionContext): Future[Unit] = {
    val promise = Promise[Unit]
    system.scheduler.scheduleOnce(10.seconds) {
      promise.trySuccess(())
    }
    promise.future
  }

  "ReactiveCouchbase ReactiveStreams Edition" should "work" in {

    implicit val system       = ActorSystem("ReactiveCouchbaseSystem")
    implicit val materializer = ActorMaterializer.create(system)
    implicit val ec           = system.dispatcher

    val useRBAC: Boolean = Try(sys.env("USE_RBAC").toBoolean).getOrElse(false)
    val configString: String = {
      if (useRBAC) {
        """
          |buckets {
          |  default {
          |    name = "default"
          |    hosts = ["127.0.0.1"]
          |    authentication = {
          |      username = "Administrator"
          |      password = "Administrator"
          |    }
          |  }
          |}
        """.stripMargin
      } else {
        """
          |buckets {
          |  default {
          |    name = "default"
          |    hosts = ["127.0.0.1"]
          |  }
          |}
        """.stripMargin
      }
    }

    val driver = ReactiveCouchbase(ConfigFactory.parseString(configString))

    val bucket = driver.bucket("default")
    bucket.withManager(_.flush()).await

    bucket.withManager(_.createN1qlPrimaryIndex(true, false)).await

    bucket
      .insert[JsValue]("key1", Json.obj("message" -> "Hello World", "type" -> "doc"))
      .await
      .debug("Insert1", a => Json.prettyPrint(a))
    bucket
      .insert[JsValue]("key2", Json.obj("message" -> "Goodbye World", "type" -> "doc"))
      .await
      .debug("Insert2", a => Json.prettyPrint(a))

    implicit val format: JsonFormat[TestModel]   = Json.format[TestModel]
    implicit val encoder: Encoder[TestModel2]    = deriveEncoder
    implicit val decoder: Decoder[TestModel2]    = deriveDecoder
    implicit val format2: JsonFormat[TestModel2] = createCBFormat

    val maybeDoc1         = bucket.get[JsValue]("key1").await.debug("maybeDoc1")
    val maybeDoc1AsClass  = bucket.get[TestModel]("key1").await.debug("maybeDoc1AsClass")
    val maybeDoc1AsClass2 = bucket.get[TestModel2]("key1").await.debug("maybeDoc1AsClass2")
    val maybeDoc2         = bucket.get[JsValue]("key2").await.debug("maybeDoc2")
    val maybeDoc3         = bucket.get[JsValue]("key3").await.debug("maybeDoc3")

    maybeDoc1 should be(Some(Json.obj("message" -> "Hello World", "type" -> "doc")))
    maybeDoc1AsClass should be(Some(TestModel("Hello World", Some("doc"))))
    maybeDoc1AsClass2 should be(Some(TestModel2("Hello World", Some("doc"))))
    maybeDoc2 should be(Some(Json.obj("message" -> "Goodbye World", "type" -> "doc")))
    maybeDoc3 should be(None)

    val doc1Exists = bucket.exists("key1").await.debug("doc1Exists")
    val doc2Exists = bucket.exists("key2").await.debug("doc2Exists")

    doc1Exists should be(true)
    doc2Exists should be(true)

    timeout(10.seconds).await

    val results1 = bucket.search[JsValue](N1qlQuery("select message from default")).asSeq.await.debug("results1")
    val results1AsClass =
      bucket.search[TestModel](N1qlQuery("select message from default")).asSeq.await.debug("resultsAsClass")
    val results1AsClass2 =
      bucket.search[TestModel2](N1qlQuery("select message from default")).asSeq.await.debug("resultsAsClass2")
    val results2 = bucket
      .search[JsValue](N1qlQuery("select message from default where message = 'Hello World'"))
      .asSeq
      .await
      .debug("results2")
    val results3 =
      bucket.search[JsValue](N1qlQuery("select message from default where type = 'doc'")).asSeq.await.debug("results3")
    val results4 = bucket
      .search[JsValue](
        N1qlQuery("select message from default where type = $type").on(Json.obj("type" -> "doc").asQueryParams)
      )
      .asSeq
      .await
      .debug("results4")
    val results5 = bucket
      .search[JsValue](
        N1qlQuery("select message from default where type = $type").on(Json.obj("type" -> "doc").asQueryParams)
      )
      .asSource
      .map(doc => (doc \ "message").as[String].toUpperCase)
      .runWith(Sink.seq[String])
      .await
      .debug("results5")

    results1 should be(Seq(Json.obj("message" -> "Hello World"), Json.obj("message" -> "Goodbye World")))
    results1AsClass should be(Seq(TestModel("Hello World", None), TestModel("Goodbye World", None)))
    results1AsClass2 should be(Seq(TestModel2("Hello World", None), TestModel2("Goodbye World", None)))
    results2 should be(Seq(Json.obj("message" -> "Hello World")))
    results3 should be(Seq(Json.obj("message" -> "Hello World"), Json.obj("message" -> "Goodbye World")))
    results4 should be(Seq(Json.obj("message" -> "Hello World"), Json.obj("message" -> "Goodbye World")))
    results5 should be(Seq("HELLO WORLD", "GOODBYE WORLD"))

    val counter1 = bucket.counter("counter", initialValue = 1L).await.debug("counter1")
    val counter2 = bucket.incr("counter").await.debug("counter2")
    val counter3 = bucket.incr("counter").await.debug("counter3")
    val counter4 = bucket.decr("counter").await.debug("counter4")
    val counter  = bucket.counterValue("counter").await.debug("counter5")

    counter1 should be(1L)
    counter2 should be(2L)
    counter3 should be(3L)
    counter4 should be(2L)
    counter should be(2L)

    val (cancel, fu) = Source
      .tick(
        FiniteDuration(0, TimeUnit.MILLISECONDS),
        FiniteDuration(400, TimeUnit.MILLISECONDS),
        NotUsed
      )
      .via(
        bucket.tailSearchFlow[JsValue](
          l => N1qlQuery(s"select message, date from default where type = 'timedoc' and date > $l"),
          (json, last) => (json \ "date").asOpt[Long].getOrElse(last)
        )
      )
      .toMat(Sink.foreach { item =>
        println("found item 1 : " + Json.prettyPrint(item))
      })(Keep.both)
      .run()(materializer)

    system.scheduler.scheduleOnce(FiniteDuration(2, TimeUnit.SECONDS))({
      cancel.cancel()
    })(ec)

    fu.andThen {
      case _ => println("done 1")
    }

    bucket
      .tailSearch[JsValue](
        l => N1qlQuery(s"select message, date from default where type = 'timedoc' and date > $l"),
        (json, last) => (json \ "date").asOpt[Long].getOrElse(last),
        limit = 5
      )
      .runWith(Sink.foreach { item =>
        println("found item 2 : " + Json.prettyPrint(item))
      })
      .andThen {
        case _ => println("done 2")
      }

    Source
      .apply(collection.immutable.Seq(1, 2, 3, 4, 5, 6))
      .flatMapConcat { v =>
        val p = Promise[Int]
        system.scheduler.scheduleOnce(FiniteDuration(300, TimeUnit.MILLISECONDS))(p.trySuccess(v))(system.dispatcher)
        Source.fromFuture(p.future)
      }
      .map(v => {
        (s"doc-$v", Json.obj("type" -> "timedoc", "message" -> s"message nb $v", "date" -> System.nanoTime()))
      })
      .via(bucket.insertFlow[JsValue]())
      .runWith(Sink.seq)
      .await
      .debug("Res")

    bucket.withManager(_.flush()).await
    bucket.close().await
    system.terminate
  }

  "ReactiveCouchbase ReactiveStreams Edition" should "work with data structures" in {
    implicit val system       = ActorSystem("ReactiveCouchbaseSystem")
    implicit val materializer = ActorMaterializer.create(system)
    implicit val ec           = system.dispatcher

    val useRBAC: Boolean = Try(sys.env("USE_RBAC").toBoolean).getOrElse(false)
    val configString: String = {
      if (useRBAC) {
        """
          |buckets {
          |  default {
          |    name = "default"
          |    hosts = ["127.0.0.1"]
          |    authentication = {
          |      username = "Administrator"
          |      password = "Administrator"
          |    }
          |  }
          |}
        """.stripMargin
      } else {
        """
          |buckets {
          |  default {
          |    name = "default"
          |    hosts = ["127.0.0.1"]
          |  }
          |}
        """.stripMargin
      }
    }

    val driver = ReactiveCouchbase(ConfigFactory.parseString(configString))

    val bucket = driver.bucket("default")
    bucket.withManager(_.flush()).await

    bucket.withManager(_.createN1qlPrimaryIndex(true, false)).await

    bucket.remove("data.structures.list").recover { case _  => Json.obj() }.await
    bucket.remove("data.structures.set").recover { case _   => Json.obj() }.await
    bucket.remove("data.structures.queue").recover { case _ => Json.obj() }.await
    bucket.remove("data.structures.map").recover { case _   => Json.obj() }.await

    bucket.insert[JsValue]("data.structures.list", Json.arr()).await
    bucket.insert[JsValue]("data.structures.queue", Json.arr()).await
    bucket.insert[JsValue]("data.structures.set", Json.arr()).await
    bucket.insert[JsValue]("data.structures.map", Json.obj()).await

    bucket.lists.append("data.structures.list", "hello").await shouldEqual true
    bucket.lists.append("data.structures.list", "bye").await shouldEqual true
    bucket.lists.get("data.structures.list", 0).await shouldEqual Some("hello")
    bucket.lists.prepend("data.structures.list", "yo").await shouldEqual true
    bucket.lists.get("data.structures.list", 0).await shouldEqual Some("yo")
    bucket.lists.size("data.structures.list").await shouldEqual 3

    bucket.lists.get("data.structures.list", 10).await shouldEqual None

    bucket.sets.add("data.structures.set", "hello").await shouldEqual true
    bucket.sets.add("data.structures.set", "bye").await shouldEqual true
    bucket.sets.contains("data.structures.set", "hello").await shouldEqual true
    bucket.sets.contains("data.structures.set", "bye").await shouldEqual true
    bucket.sets.size("data.structures.set").await shouldEqual 2
    bucket.sets.remove("data.structures.set", "qsddsdqs").await shouldEqual Some("qsddsdqs")

    bucket.queues.push("data.structures.queue", "1").await shouldEqual true
    bucket.queues.push("data.structures.queue", "2").await shouldEqual true
    bucket.queues.push("data.structures.queue", "3").await shouldEqual true
    bucket.queues.size("data.structures.queue").await shouldEqual 3
    bucket.queues.pop("data.structures.queue").await shouldEqual Some("1")
    bucket.queues.pop("data.structures.queue").await shouldEqual Some("2")
    bucket.queues.pop("data.structures.queue").await shouldEqual Some("3")
    bucket.queues.pop("data.structures.queue").await shouldEqual None

    bucket.maps.put("data.structures.map", "key1", "value1").await shouldEqual true
    bucket.maps.put("data.structures.map", "key2", "value2").await shouldEqual true
    bucket.maps.put("data.structures.map", "key2", "value11").await shouldEqual true

    bucket.maps.get[String]("data.structures.map", "key1").await shouldEqual Some("value1")
    bucket.maps.get[String]("data.structures.map", "key2").await shouldEqual Some("value11")
    bucket.maps.get[String]("data.structures.map", "key3").await shouldEqual None

    bucket.withManager(_.flush()).await
    bucket.close().await
    system.terminate
  }

  "ReactiveCouchbase ReactiveStreams Edition" should "work with view search" in {
    implicit val system       = ActorSystem("ReactiveCouchbaseSystem")
    implicit val materializer = ActorMaterializer.create(system)
    implicit val ec           = system.dispatcher

    val useRBAC: Boolean = Try(sys.env("USE_RBAC").toBoolean).getOrElse(false)
    val configString: String = {
      if (useRBAC) {
        """
          |buckets {
          |  default {
          |    name = "default"
          |    hosts = ["127.0.0.1"]
          |    authentication = {
          |      username = "Administrator"
          |      password = "Administrator"
          |    }
          |  }
          |}
        """.stripMargin
      } else {
        """
          |buckets {
          |  default {
          |    name = "default"
          |    hosts = ["127.0.0.1"]
          |  }
          |}
        """.stripMargin
      }
    }

    implicit val encoderPerson: Encoder[Person]      = deriveEncoder
    implicit val decoderPerson: Decoder[Person]      = deriveDecoder
    implicit val formatterPerson: JsonFormat[Person] = createCBFormat(encoderPerson, decoderPerson)

    val designDocumentName = "persons"
    val userViewName = "by_name"

    val driver = ReactiveCouchbase(ConfigFactory.parseString(configString))

    val bucket = driver.bucket("default")

    bucket.withManager(_.flush()).await

    bucket.withManager(_.createN1qlPrimaryIndex(true, false)).await

    bucket.withManager(_.insertDesignDocument(DesignDocument.from(designDocumentName, JsonObject.fromJson(
      s"""
         |{
         |    "views":{
         |       "$userViewName": {
         |           "map": "function (doc, meta) { emit(doc.name, null); } "
         |       },
         |       "by_surname": {
         |           "map": "function (doc, meta) { emit(doc.surname, null); } "
         |       },
         |       "by_age": {
         |           "map": "function (doc, meta) { emit(doc.age, null); } "
         |       }
         |    }
         |}
       """.stripMargin))))
    for(i <- 0 to 10) {
      bucket.insert(s"person--$i", Person("Billy", s"Doe-$i", i)).await
    }

    timeout(10.seconds).await

    val users = bucket.searchView[Person](ViewQuery(
      designDocumentName,
      userViewName,
      _.includeDocs().stale(Stale.FALSE)
    )).flatMap(viewRow => Source.fromFuture(viewRow.typed)).asSeq.await

    users.size shouldBe 11

    bucket.withManager(_.flush()).await
    bucket.close().await
    system.terminate
  }
}
