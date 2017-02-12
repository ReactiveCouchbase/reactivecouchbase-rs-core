import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import org.reactivecouchbase.scaladsl.{N1qlQuery, ReactiveCouchbase}
import org.scalatest._
import play.api.libs.json.Json

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

    bucket.remove("key1").recover { case _ => Json.obj() }.await.debug("Remove1")
    bucket.remove("key2").recover { case _ => Json.obj() }.await.debug("Remove2")

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

    bucket.close().await
  }
}