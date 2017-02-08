package org.reactivecouchbase.scaladsl

import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy}
import com.couchbase.client.java.{AsyncBucket, CouchbaseAsyncCluster}
import com.couchbase.client.java.bucket.AsyncBucketManager
import com.couchbase.client.java.document.RawJsonDocument
import com.couchbase.client.java.query.AsyncN1qlQueryRow
import com.typesafe.config.Config
import org.reactivecouchbase.scaladsl.Implicits._
import play.api.libs.json._
import rx.functions.{Action0, Action1}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

case class BucketConfig(name: String, password: Option[String] = None, hosts: Seq[String])

object BucketConfig {
  import collection.JavaConversions._
  def apply(config: Config, system: ActorSystem): BucketConfig = {
    val name = Try(config.getString("name")).get
    val password = Try(config.getString("password")).toOption
    val hosts = Try(config.getStringList("hosts")).get.toIndexedSeq
    BucketConfig(name, password, hosts)
  }
}

object Bucket {
  def apply(config: BucketConfig) = new Bucket(config)
}

class Bucket(config: BucketConfig) {

  private val defaultReads: Reads[JsValue] = Reads.apply(jsv => JsSuccess(jsv))
  private val defaultWrites: Writes[JsValue] = Writes.apply(jsv => jsv)
  private val defaultWriteSettings: WriteSettings = WriteSettings()
  private val internalExecutionContext = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  private val cluster: CouchbaseAsyncCluster = CouchbaseAsyncCluster.create(config.hosts:_*)
  private val futureBucket: Future[AsyncBucket] = config.password
    .map(p => cluster.openBucket(config.name, p).asFuture)
    .getOrElse(cluster.openBucket(config.name).asFuture)
    .flatMap(bucket =>
      bucket.bucketManager().asFuture
        .map(_.createN1qlPrimaryIndex(true, false))(internalExecutionContext)
        .map(_ => bucket)(internalExecutionContext))(internalExecutionContext) // TODO : avoid index creation

  // TODO
  // implement other searches
  // implement management (design doc, etc ...)

  // TODO streams
  // get
  // insert
  // upsert
  // remove

  // TODO later
  // maps operations
  // sets operations
  // lists operations
  // queues operations
  // getAndTouch
  // getAndLock
  // counter operations

  def close(implicit ec: ExecutionContext): Future[Boolean] = futureBucket.flatMap(_.close().asFuture.map(_.booleanValue()))

  def manager(implicit ec: ExecutionContext): Future[AsyncBucketManager] = {
    futureBucket.flatMap(b => b.bucketManager().asFuture)
  }

  def exists(key: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    futureBucket.flatMap { bucket =>
      bucket.exists(key).asFuture.map(_.booleanValue())
    }
  }

  def remove(key: String)(implicit ec: ExecutionContext): Future[JsValue] = {
    futureBucket.flatMap { bucket =>
      bucket.remove(RawJsonDocument.create(key)).asFuture.map(doc => Json.parse(doc.content()))
    }
  }

  def insert[T](key: String, slug: T, settings: WriteSettings = defaultWriteSettings, writer: Writes[T] = defaultWrites)(implicit ec: ExecutionContext): Future[JsValue] = {
    futureBucket.flatMap { bucket =>
      bucket.insert(
        RawJsonDocument.create(
          key,
          settings.expiration.toMillis.toInt,
          Json.stringify(writer.writes(slug))
        ),
        settings.persistTo,
        settings.replicateTo
      ).asFuture.map(doc => Json.parse(doc.content()))
    }
  }

  def upsert[T](key: String, slug: T, settings: WriteSettings = defaultWriteSettings, writer: Writes[T] = defaultWrites)(implicit ec: ExecutionContext): Future[JsValue] = {
    futureBucket.flatMap { bucket =>
      bucket.upsert(
        RawJsonDocument.create(
          key,
          settings.expiration.toMillis.toInt,
          Json.stringify(writer.writes(slug))
        ),
        settings.persistTo,
        settings.replicateTo
      ).asFuture.map(doc => Json.parse(doc.content()))
    }
  }

  def get[T](key: String, reader: Reads[T] = defaultReads)(implicit ec: ExecutionContext): Future[Option[T]] = {
    futureBucket.flatMap(b => b.get(RawJsonDocument.create(key)).asFuture)
      .filter(_ != null)
      .map(doc => Json.parse(doc.content()))
      .map(jsDoc => reader.reads(jsDoc).asOpt)
  }

  def search[T](query: QueryLike, reader: Reads[T] = defaultReads)(implicit ec: ExecutionContext, materializer: Materializer): QueryResult[T] = {
    SimpleQueryResult(() => {
      val source = Source.queue[T](10000, OverflowStrategy.fail)
      val (sourceQueue, future) = source.toMat(Sink.foreach(_ => ()))(Keep.both).run()
      query match {
        case N1qlQuery(n1ql, _) => {
          futureBucket.flatMap { bucket =>
            bucket.query(com.couchbase.client.java.query.N1qlQuery.simple(n1ql)).asFuture.map { res =>
              val rows = res.rows()
              rows.doOnCompleted(new Action0 {
                override def call(): Unit = sourceQueue.complete()
              })
              rows.doOnNext(new Action1[AsyncN1qlQueryRow] {
                override def call(t: AsyncN1qlQueryRow): Unit = {
                  // TODO : better perfs here
                  val json = Json.parse(t.value().toString)
                  reader.reads(json) match {
                    case JsSuccess(s, _) => sourceQueue.offer(s)
                    case JsError(e) => sourceQueue.fail(new RuntimeException("Error while parsing document")) // TODO : better error
                  }
                }
              })
            }
          }
        }
        case _ => Source.failed(new UnsupportedOperationException("Query not supported !"))
      }
      source
    })
  }
}
