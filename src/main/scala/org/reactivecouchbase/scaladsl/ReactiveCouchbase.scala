package org.reactivecouchbase.scaladsl

import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}

class ReactiveCouchbase(val config: Config, val system: ActorSystem) {

  private val pool = new ConcurrentHashMap[String, Bucket]()

  def bucket(name: String): Bucket = {
    pool.computeIfAbsent(name, JavaUtils.function { key =>
      Bucket(BucketConfig(config.getConfig(s"buckets.$key"), system))
    })
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