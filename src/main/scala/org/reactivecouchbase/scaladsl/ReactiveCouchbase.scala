package org.reactivecouchbase.scaladsl

import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Future

class ReactiveCouchbase(val config: Config, val system: ActorSystem) {

  private val pool = new ConcurrentHashMap[String, Bucket]()

  def bucket(name: String): Bucket = {
    pool.computeIfAbsent(name, JavaUtils.function { key =>
      Bucket(BucketConfig(config.getConfig(s"buckets.$key"), system), () => pool.remove(name))
    })
  }

  def terminate(): Future[Unit] = {

    import collection.JavaConversions._

    implicit val ec = system.dispatcher

    system.terminate().flatMap { _ =>
      Future.sequence(pool.toSeq.map(_._2.close())).map(_ => ())
    }
  }
}

object ReactiveCouchbase {
  def apply(config: Config): ReactiveCouchbase = {
    val actualConfig = config.withFallback(ConfigFactory.parseString("akka {}"))
    new ReactiveCouchbase(actualConfig, ActorSystem("ReactiveCouchbaseSystem", actualConfig.getConfig("akka")))
  }
  def apply(config: Config, system: ActorSystem): ReactiveCouchbase = {
    val actualConfig = config.withFallback(ConfigFactory.empty())
    new ReactiveCouchbase(actualConfig, system)
  }
}