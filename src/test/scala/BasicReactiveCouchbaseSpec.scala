import java.util.concurrent.TimeUnit

import akka.{Done, NotUsed}
import akka.actor.{ActorSystem, Cancellable}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.couchbase.client.java.view.Stale
import com.typesafe.config.ConfigFactory
import org.reactivecouchbase.rs.scaladsl.{N1qlQuery, ReactiveCouchbase, ViewQuery}
import org.reactivecouchbase.rs.scaladsl.json._
import org.scalatest._
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration

class BasicReactiveCouchbaseSpec extends FlatSpec with Matchers {

  import TestImplicits._

  implicit val system = ActorSystem("ReactiveCouchbaseSystem")
  implicit val materializer = ActorMaterializer.create(system)
  implicit val ec = system.dispatcher

  "ReactiveCouchbase ReactiveStreams Edition" should "work" in {

    val driver = ReactiveCouchbase(ConfigFactory.parseString(
      """
        |buckets {
        |  default {
        |    name = "default"
        |    hosts = ["127.0.0.1"]
        |  }
        |}
      """.stripMargin), system)

    val bucket = driver.bucket("default")

    bucket.remove("doc-1").recover { case _ => Json.obj() }.await
    bucket.remove("doc-2").recover { case _ => Json.obj() }.await
    bucket.remove("doc-3").recover { case _ => Json.obj() }.await
    bucket.remove("doc-4").recover { case _ => Json.obj() }.await
    bucket.remove("doc-5").recover { case _ => Json.obj() }.await
    bucket.remove("doc-6").recover { case _ => Json.obj() }.await
    bucket.remove("key1").recover { case _ => Json.obj() }.await.debug("Remove1")
    bucket.remove("key2").recover { case _ => Json.obj() }.await.debug("Remove2")
    bucket.remove("counter").recover { case _ => Json.obj() }.await.debug("Remove3")

    bucket.insert("key1", Json.obj("message" -> "Hello World", "type" -> "doc")).await.debug("Insert1", a => Json.prettyPrint(a))
    bucket.insert("key2", Json.obj("message" -> "Goodbye World", "type" -> "doc")).await.debug("Insert2", a => Json.prettyPrint(a))

    val maybeDoc1 = bucket.get("key1").await.debug("maybeDoc1")
    val maybeDoc2 = bucket.get("key2").await.debug("maybeDoc2")
    val maybeDoc3 = bucket.get("key3").await.debug("maybeDoc3")

    maybeDoc1 should be (Some(Json.obj("message" -> "Hello World", "type" -> "doc")))
    maybeDoc2 should be (Some(Json.obj("message" -> "Goodbye World", "type" -> "doc")))
    maybeDoc3 should be (None)

    val doc1Exists = bucket.exists("key1").await.debug("doc1Exists")
    val doc2Exists = bucket.exists("key2").await.debug("doc2Exists")

    doc1Exists should be (true)
    doc2Exists should be (true)

    val results1 = bucket.search(N1qlQuery("select message from default")).asSeq.await.debug("results1")
    val results2 = bucket.search(N1qlQuery("select message from default where message = 'Hello World'")).asSeq.await.debug("results2")
    val results3 = bucket.search(N1qlQuery("select message from default where type = 'doc'")).asSeq.await.debug("results3")
    val results4 = bucket.search(N1qlQuery("select message from default where type = $type").on(Json.obj("type" -> "doc"))).asSeq.await.debug("results4")
    val results5 = bucket.search(N1qlQuery("select message from default where type = $type'").on(Json.obj("type" -> "doc")))
      .asSource.map(doc => (doc \ "message").as[String].toUpperCase)
      .runWith(Sink.seq[String]).await.debug("results5")

    results1 should be (Seq(Json.obj("message" -> "Hello World"), Json.obj("message" -> "Goodbye World")))
    results2 should be (Seq(Json.obj("message" -> "Hello World")))
    results3 should be (Seq(Json.obj("message" -> "Hello World"), Json.obj("message" -> "Goodbye World")))
    results4 should be (Seq(Json.obj("message" -> "Hello World"), Json.obj("message" -> "Goodbye World")))
    results5 should be (Seq("HELLO WORLD", "GOODBYE WORLD"))

    val counter1 = bucket.counter("counter", initialValue = 1L).await.debug("counter1")
    val counter2 = bucket.incr("counter").await.debug("counter2")
    val counter3 = bucket.incr("counter").await.debug("counter3")
    val counter4 = bucket.decr("counter").await.debug("counter4")
    val counter  = bucket.counterValue("counter").await.debug("counter5")

    counter1 should be (1L)
    counter2 should be (2L)
    counter3 should be (3L)
    counter4 should be (2L)
    counter  should be (2L)

    val (cancel, fu) = Source.tick(
      FiniteDuration(0, TimeUnit.MILLISECONDS),
      FiniteDuration(400, TimeUnit.MILLISECONDS),
      NotUsed
    ).via(bucket.tailSearchFlow[JsValue](
      l => N1qlQuery(s"select message, date from default where type = 'timedoc' and date > $l"),
      (json, last) => (json \ "date").asOpt[Long].getOrElse(last)
    )).toMat(Sink.foreach { item =>
      println("found item 1 : " + Json.prettyPrint(item))
    })(Keep.both).run()(materializer)

    system.scheduler.scheduleOnce(FiniteDuration(2, TimeUnit.SECONDS))({
      cancel.cancel()
    })(ec)

    fu.andThen {
      case _ => println("done 1")
    }

    bucket.tailSearch[JsValue](
      l => N1qlQuery(s"select message, date from default where type = 'timedoc' and date > $l"),
      (json, last) => (json \ "date").asOpt[Long].getOrElse(last),
      limit = 5
    ).runWith(Sink.foreach { item =>
      println("found item 2 : " + Json.prettyPrint(item))
    }).andThen {
      case _ => println("done 2")
    }

    Source.apply(collection.immutable.Seq(1, 2, 3, 4, 5, 6)).flatMapConcat { v =>
      val p = Promise[Int]
      system.scheduler.scheduleOnce(FiniteDuration(300, TimeUnit.MILLISECONDS))(p.trySuccess(v))(system.dispatcher)
      Source.fromFuture(p.future)
    }.map(v => {
      (s"doc-$v", Json.obj("type" -> "timedoc", "message" -> s"message nb $v", "date" -> System.nanoTime()))
    }).via(bucket.insertFlow[JsValue]()).runWith(Sink.seq).await.debug("Res")

    val usersUnder43: Future[Seq[JsValue]] = bucket.searchView(
      ViewQuery("persons", "by_age", _.stale(Stale.FALSE).includeDocs(true).startKey(0).endKey(42))
    ).flatMap(d => Source.fromFuture(d.doc)).asSeq

    bucket.close().await
  }
}