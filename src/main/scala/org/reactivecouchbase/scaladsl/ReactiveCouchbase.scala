package org.reactivecouchbase.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import play.api.libs.json.Json

class ReactiveCouchbase(val config: Config, val system: ActorSystem) {
  def bucket(name: String): Bucket = Bucket(BucketConfig(config.getConfig(s"buckets.$name"), system))
}

object ReactiveCouchbase {
  def apply(config: Config): ReactiveCouchbase = {
    val actualConfig = config.withFallback(ConfigFactory.parseString("akka {}"))
    new ReactiveCouchbase(actualConfig, ActorSystem("ReactiveCouchbaseSystem", actualConfig.getConfig("akka")))
  }
  def apply(config: Config, system: ActorSystem): ReactiveCouchbase = {
    val actualConfig = config.withFallback(ConfigFactory.parseString("akka {}"))
    new ReactiveCouchbase(actualConfig, system)
  }
}

object ReactiveCouchbaseApp extends App {

  import Implicits._

  implicit val system = ActorSystem("MyActorSystem")
  implicit val materializer = ActorMaterializer.create(system)
  implicit val ec = system.dispatcher

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

  bucket.insert("key1", Json.obj("message" -> "Hello World")).await
  bucket.insert("key2", Json.obj("message" -> "Goodbye World")).await

  val maybeDoc1 = bucket.get("key1").await.debug("maybeDoc1")
  val maybeDoc2 = bucket.get("key2").await.debug("maybeDoc2")

  val doc1Exists = bucket.exists("key1").await.debug("doc1Exists")
  val doc2Exists = bucket.exists("key2").await.debug("doc2Exists")

  val results1 = bucket.search(N1qlQuery("select message from default")).asSeq.await.debug("results1")
  val results2 = bucket.search(N1qlQuery("select message from default where message = 'Hello World'")).asSeq.await.debug("results2")

  bucket.remove("key1").await
  bucket.remove("key2").await

  bucket.close.await
}