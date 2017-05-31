package org.reactivecouchbase.scaladsl

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.bucket.AsyncBucketManager
import com.couchbase.client.java.document.{JsonLongDocument, RawJsonDocument}
import com.typesafe.config.Config
import org.reactivecouchbase.scaladsl.Implicits._
import org.reactivecouchbase.scaladsl.json.PlayJsonDocument
import org.reactivecouchbase.scaladsl.json.PlayJsonDocument.DocumentPimps
import org.reactivestreams.Publisher
import play.api.libs.json._
import rx.{Observable, RxReactiveStreams}

import scala.concurrent.duration.FiniteDuration
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
  def apply(config: BucketConfig, onStop: () => Unit) = new Bucket(config, onStop)
}

class Bucket(config: BucketConfig, onStop: () => Unit) {

  private val defaultWriteSettings: WriteSettings = WriteSettings()

  // TODO : pass complex settings
  private val cluster: CouchbaseCluster = CouchbaseCluster.create(config.hosts:_*)

  // TODO : implements in a non blocking fashion
  private val (bucket, asyncBucket, bucketManager, futureBucket) = {
    val _bucket = config.password
      .map(p => cluster.openBucket(config.name, p))
      .getOrElse(cluster.openBucket(config.name))
    val _bucketManager = _bucket.bucketManager()
    // TODO : avoid index creation
    _bucketManager.async().createN1qlPrimaryIndex(true, false)
    (_bucket, _bucket.async(), _bucketManager, Future.successful(_bucket.async()))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // TODO : map clusterManager
  // TODO : map bucketManager
  def underlyingCluster = cluster
  def underlyingBucket = bucket
  def underlyingAsyncBucket = asyncBucket
  def underlyingBucketManager = bucketManager

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def tailSearchFlow[T](query: Long => QueryLike, extractor: (T, Long) => Long, from: Long = 0L)(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Flow[NotUsed, T, NotUsed] = {
    // TODO : rewrite for perfs
    // TODO : implement for other kind of search
    val ref = new AtomicReference[Long](from)
    val last = new AtomicReference[T]()
    Flow[NotUsed].flatMapConcat { _ =>
      search[T](query(ref.get()))
        .asSource
        .map { item =>
          last.set(item)
          item
        }
        .alsoTo(Sink.onComplete { _ =>
          ref.set(extractor(last.get(), ref.get()))
        })
    }
  }

  def tailSearch[T](query: Long => QueryLike, extractor: (T, Long) => Long, from: Long = 0L, limit: Long = Long.MaxValue, every: FiniteDuration = FiniteDuration(200, TimeUnit.MILLISECONDS))(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Source[T, Cancellable] = {
    // TODO : implement for other kind of search
    Source.tick(
      FiniteDuration(0, TimeUnit.MILLISECONDS),
      every,
      NotUsed
    ).via(tailSearchFlow[T](query, extractor, from)).take(limit)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def getFlow[T]()(implicit ec: ExecutionContext, reader: Reads[T]): Flow[String, T, NotUsed] = {
    Flow[String].flatMapConcat(k => Source.fromFuture(get[T](k))).filter(_.isDefined).map(_.get)
  }

  def getFromSource[T, Mat](keys: Source[String, Mat])(implicit ec: ExecutionContext, reader: Reads[T]): Source[T, Mat] = {
    keys.via(getFlow[T]())
  }

  def getFromPublisher[T](keys: Publisher[String])(implicit ec: ExecutionContext, reader: Reads[T]): Source[T, NotUsed] = {
    getFromSource[T, NotUsed](Source.fromPublisher(keys))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def removeFlow(settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext): Flow[String, Boolean, NotUsed] = {
    Flow[String].flatMapConcat(k => Source.fromFuture(remove(k, settings)(ec)))
  }

  def removeFromSource[Mat](keys: Source[String, Mat], settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext): Source[Boolean, Mat] = {
    keys.via(removeFlow(settings)(ec))
  }

  def removeFromPublisher(keys: Publisher[String], settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext): Source[Boolean, NotUsed] = {
    removeFromSource[NotUsed](Source.fromPublisher(keys), settings)(ec)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def insertFlow[T](settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Flow[(String, T), T, NotUsed] = {
    Flow[(String, T)].flatMapConcat(tuple => Source.fromFuture(insert[T](tuple._1, tuple._2, settings)))
  }

  def insertFromSource[T, Mat](values: Source[(String, T), Mat], settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Source[T, Mat] = {
    values.via(insertFlow[T](settings))
  }

  def insertFromPublisher[T](values: Publisher[(String, T)], settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Source[T, NotUsed] = {
    insertFromSource[T, NotUsed](Source.fromPublisher(values), settings)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def upsertFlow[T](settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Flow[(String, T), T, NotUsed] = {
    Flow[(String, T)].flatMapConcat(tuple => Source.fromFuture(upsert[T](tuple._1, tuple._2, settings)))
  }

  def upsertFromSource[T, Mat](values: Source[(String, T), Mat], settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Source[T, Mat] = {
    values.via(upsertFlow[T](settings))
  }

  def upsertFromPublisher[T](values: Publisher[(String, T)], settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Source[T, NotUsed] = {
    upsertFromSource[T, NotUsed](Source.fromPublisher(values), settings)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // TODO : implements for other searches
  def searchFlow[T]()(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Flow[QueryLike, T, NotUsed] = {
    Flow[QueryLike].flatMapConcat(query => search[T](query).asSource)
  }

  def searchFromSource[T, Mat](query: Source[QueryLike, Mat])(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Source[T, Mat] = {
    query.via(searchFlow[T]())
  }

  def searchFromPublisher[T](query: Publisher[QueryLike])(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Source[T, NotUsed] = {
    searchFromSource[T, NotUsed](Source.fromPublisher(query))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // TODO : maps operations
  // TODO : sets operations
  // TODO : lists operations
  // TODO : queues operations
  // TODO : getAndTouch
  // TODO : lock operations

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def incr(key: String, settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext): Future[Long] = counter(key, 1L, 0L, settings)(ec)

  def decr(key: String, settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext): Future[Long] = counter(key, -1L, 0L, settings)(ec)

  def counter(key: String, delta: Long = 0L, initialValue: Long = 0L, settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext): Future[Long] = {
    futureBucket.flatMap { bucket =>
      bucket.counter(key, delta, initialValue, settings.persistTo, settings.replicateTo).asFuture.map(_.content())
    }
  }

  def counterValue(key: String, settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext): Future[Long] = {
    futureBucket.flatMap { bucket =>
      bucket.get(JsonLongDocument.create(key)).asFuture.filter(_ != null).map(_.content())
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def close()(implicit ec: ExecutionContext): Future[Boolean] = {
    onStop()
    futureBucket.flatMap(_.close().asFuture.map(_.booleanValue())).andThen {
      case _ => cluster.disconnect()
    }
  }

  def manager(implicit ec: ExecutionContext): Future[AsyncBucketManager] = {
    futureBucket.flatMap(b => b.bucketManager().asFuture)
  }

  def exists(key: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    futureBucket.flatMap { bucket =>
      bucket.exists(key).asFuture.map(_.booleanValue())
    }
  }

  def remove(key: String, settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext): Future[Boolean] = {
    futureBucket.flatMap { bucket =>
      bucket.remove(key, settings.persistTo, settings.replicateTo).asFuture.map(_ != null)
    }
  }

  def insert[T](key: String, slug: T, settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Future[T] = {
    futureBucket.flatMap { bucket =>
      bucket.insert(
        PlayJsonDocument.create(
          key,
          settings.expiration.toMillis.toInt,
          slug
        ),
        settings.persistTo,
        settings.replicateTo
      ).asFuture.map(_.value)
    }
  }

  def upsert[T](key: String, slug: T, settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Future[T] = {
    futureBucket.flatMap { bucket =>
      bucket.upsert(
        PlayJsonDocument.create(
          key,
          settings.expiration.toMillis.toInt,
          slug
        ),
        settings.persistTo,
        settings.replicateTo
      ).asFuture.map(_.value)
    }
  }

  def get[T](key: String)(implicit ec: ExecutionContext, reader: Reads[T]): Future[Option[T]] = {
    futureBucket.flatMap(b => b.get(PlayJsonDocument.create(key)).asFuture)
      .filter(_ != null)
      .map(_.jsResult.asOpt).recoverWith {
        case ObservableCompletedWithoutValue => Future.successful(None)
      }
  }

  def searchSpatial[T](query: SpatialQuery)(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): QueryResult[SpatialViewRow[T], NotUsed] = {
    SimpleQueryResult(() => {
      val obs: Observable[SpatialViewRow[T]] = asyncBucket.query(query.query)
        .flatMap(RxUtils.func1(_.rows()))
        .map(RxUtils.func1 { row =>
          SpatialViewRow[T](
            row.id(),
            JsonConverter.convertToJsValue(row.key()),
            JsonConverter.convertToJsValue(row.value()),
            JsonConverter.convertToJsValue(row.geometry()),
            row.document(classOf[RawJsonDocument]).asFuture.filter(_ != null).map(a => Json.parse(a.content())),
            reader
          )
        })
      Source.fromPublisher[SpatialViewRow[T]](RxReactiveStreams.toPublisher[SpatialViewRow[T]](obs))
    })
  }

  def searchView[T](query: ViewQuery)(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): QueryResult[ViewRow[T], NotUsed] = {
    SimpleQueryResult(() => {
      val obs: Observable[ViewRow[T]] = asyncBucket.query(query.query)
        .flatMap(RxUtils.func1(_.rows()))
        .map(RxUtils.func1 { row =>
          ViewRow(
            row.id(),
            JsonConverter.convertToJsValue(row.key()),
            JsonConverter.convertToJsValue(row.value()),
            row.document(classOf[RawJsonDocument]).asFuture.filter(_ != null).map(a => Json.parse(a.content())),
            reader
          )
        })
      Source.fromPublisher[ViewRow[T]](RxReactiveStreams.toPublisher[ViewRow[T]](obs))
    })
  }

  def search[T](query: QueryLike)(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): QueryResult[T, NotUsed] = {
    SimpleQueryResult(() => {
      val obs: Observable[T] = query match {
        case N1qlQuery(n1ql, args) if args.value.isEmpty => {
          asyncBucket.query(com.couchbase.client.java.query.N1qlQuery.simple(n1ql))
            .flatMap(RxUtils.func1(_.rows()))
            .map(RxUtils.func1 { t =>
              reader.reads(Json.parse(t.byteValue())) match {
                case JsSuccess(s, _) => s
                case JsError(e) => throw new RuntimeException(s"Error while parsing document : $e") // TODO : better error
              }
            })
        }
        case N1qlQuery(n1ql, args) if args.value.nonEmpty => {
          val params = JsonConverter.convertToJson(args)
          asyncBucket.query(com.couchbase.client.java.query.N1qlQuery.parameterized(n1ql, params))
            .flatMap(RxUtils.func1(_.rows()))
            .map(RxUtils.func1 { t =>
              reader.reads(Json.parse(t.byteValue())) match {
                case JsSuccess(s, _) => s
                case JsError(e) => throw new RuntimeException(s"Error while parsing document : $e") // TODO : better error
              }
            })
        }
        case _ => Observable.error(new UnsupportedOperationException("Query not supported !"))
      }
      Source.fromPublisher[T](RxReactiveStreams.toPublisher[T](obs))
    })
  }
}
