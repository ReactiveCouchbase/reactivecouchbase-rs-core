package org.reactivecouchbase.rs.scaladsl

import java.util.concurrent.{ConcurrentHashMap, Executors}

import com.typesafe.config.{Config, ConfigFactory}
import org.reactivecouchbase.rs.scaladsl.TypeUtils.EnvCustomizer

import scala.concurrent.{ExecutionContext, Future}

class ReactiveCouchbase(val config: Config) {

  private val pool = new ConcurrentHashMap[String, Bucket]()

  def bucket(name: String, env: EnvCustomizer = identity): Bucket = {
    pool.computeIfAbsent(name, JavaUtils.function { key =>
      Bucket(BucketConfig(config.getConfig(s"buckets.$key"), env), () => pool.remove(name))
    })
  }

  def configureAndPoolBucket(name: String, env: EnvCustomizer): Unit = {
    pool.computeIfAbsent(name, JavaUtils.function { key =>
      Bucket(BucketConfig(config.getConfig(s"buckets.$key"), env), () => pool.remove(name))
    })
    ()
  }

  def terminate(): Future[Unit] = {

    import collection.JavaConversions._

    implicit val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

    Future.sequence(pool.toSeq.map(_._2.close())).map(_ => ())
  }
}

object ReactiveCouchbase {
  def apply(config: Config): ReactiveCouchbase = {
    val actualConfig = config.withFallback(ConfigFactory.empty())
    new ReactiveCouchbase(actualConfig)
  }
}