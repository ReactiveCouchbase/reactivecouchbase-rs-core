package org.reactivecouchbase.scaladsl

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}

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