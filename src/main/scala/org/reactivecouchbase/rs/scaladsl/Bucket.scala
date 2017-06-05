package org.reactivecouchbase.rs.scaladsl

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.couchbase.client.core.ClusterFacade
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.bucket.AsyncBucketManager
import com.couchbase.client.java.document.{JsonLongDocument, RawJsonDocument}
import com.couchbase.client.java.env.CouchbaseEnvironment
import com.couchbase.client.java.repository.AsyncRepository
import com.typesafe.config.Config
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
  private val _cluster: CouchbaseCluster = CouchbaseCluster.create(config.hosts:_*)

  // TODO : implements in a non blocking fashion
  private val (_bucket, _asyncBucket, _bucketManager, _futureBucket) = {
    val _bucket = config.password
      .map(p => _cluster.openBucket(config.name, p))
      .getOrElse(_cluster.openBucket(config.name))
    val _bucketManager = _bucket.bucketManager()
    // TODO : avoid index creation
    _bucketManager.async().createN1qlPrimaryIndex(true, false)
    (_bucket, _bucket.async(), _bucketManager, Future.successful(_bucket.async()))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def underlyingCluster = _cluster
  def underlyingBucket = _bucket
  def underlyingAsyncBucket = _asyncBucket
  def underlyingBucketManager = _bucketManager

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def tailSearchFlow[T](query: Long => QueryLike, extractor: (T, Long) => Long, from: Long = 0L)(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Flow[NotUsed, T, NotUsed] = {
    // TODO : rewrite for perfs
    val ref = new AtomicReference[Long](from)
    val last = new AtomicReference[T]()
    Flow[NotUsed].flatMapConcat { _ =>
      search[T](query(ref.get()))(ec, mat, reader)
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

  def tailViewSearchFlow[T](query: Long => ViewQuery, extractor: (ViewRow[T], Long) => Long, from: Long = 0L)(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Flow[NotUsed, ViewRow[T], NotUsed] = {
    // TODO : rewrite for perfs
    val ref = new AtomicReference[Long](from)
    val last = new AtomicReference[ViewRow[T]]()
    Flow[NotUsed].flatMapConcat { _ =>
      searchView[T](query(ref.get()))(ec, mat, reader)
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

  def tailSpatialSearchFlow[T](query: Long => SpatialQuery, extractor: (SpatialViewRow[T], Long) => Long, from: Long = 0L)(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Flow[NotUsed, SpatialViewRow[T], NotUsed] = {
    // TODO : rewrite for perfs
    val ref = new AtomicReference[Long](from)
    val last = new AtomicReference[SpatialViewRow[T]]()
    Flow[NotUsed].flatMapConcat { _ =>
      searchSpatial[T](query(ref.get()))(ec, mat, reader)
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
    Source.tick(
      FiniteDuration(0, TimeUnit.MILLISECONDS),
      every,
      NotUsed
    ).via(tailSearchFlow[T](query, extractor, from)(ec, mat, reader)).take(limit)
  }

  def tailViewSearch[T](query: Long => ViewQuery, extractor: (ViewRow[T], Long) => Long, from: Long = 0L, limit: Long = Long.MaxValue, every: FiniteDuration = FiniteDuration(200, TimeUnit.MILLISECONDS))(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Source[ViewRow[T], Cancellable] = {
    Source.tick(
      FiniteDuration(0, TimeUnit.MILLISECONDS),
      every,
      NotUsed
    ).via(tailViewSearchFlow[T](query, extractor, from)(ec, mat, reader)).take(limit)
  }

  def tailSpatialSearch[T](query: Long => SpatialQuery, extractor: (SpatialViewRow[T], Long) => Long, from: Long = 0L, limit: Long = Long.MaxValue, every: FiniteDuration = FiniteDuration(200, TimeUnit.MILLISECONDS))(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Source[SpatialViewRow[T], Cancellable] = {
    Source.tick(
      FiniteDuration(0, TimeUnit.MILLISECONDS),
      every,
      NotUsed
    ).via(tailSpatialSearchFlow[T](query, extractor, from)(ec, mat, reader)).take(limit)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def getFlow[T]()(implicit ec: ExecutionContext, reader: Reads[T]): Flow[String, T, NotUsed] = {
    Flow[String].flatMapConcat(k => Source.fromFuture(get[T](k)(ec, reader))).filter(_.isDefined).map(_.get)
  }

  def getFromSource[T, Mat](keys: Source[String, Mat])(implicit ec: ExecutionContext, reader: Reads[T]): Source[T, Mat] = {
    keys.via(getFlow[T]()(ec, reader))
  }

  def getFromPublisher[T](keys: Publisher[String])(implicit ec: ExecutionContext, reader: Reads[T]): Source[T, NotUsed] = {
    getFromSource[T, NotUsed](Source.fromPublisher(keys))(ec, reader)
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
    Flow[(String, T)].flatMapConcat(tuple => Source.fromFuture(insert[T](tuple._1, tuple._2, settings)(ec, format)))
  }

  def insertFromSource[T, Mat](values: Source[(String, T), Mat], settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Source[T, Mat] = {
    values.via(insertFlow[T](settings)(ec, format))
  }

  def insertFromPublisher[T](values: Publisher[(String, T)], settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Source[T, NotUsed] = {
    insertFromSource[T, NotUsed](Source.fromPublisher(values), settings)(ec, format)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def upsertFlow[T](settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Flow[(String, T), T, NotUsed] = {
    Flow[(String, T)].flatMapConcat(tuple => Source.fromFuture(upsert[T](tuple._1, tuple._2, settings)(ec, format)))
  }

  def upsertFromSource[T, Mat](values: Source[(String, T), Mat], settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Source[T, Mat] = {
    values.via(upsertFlow[T](settings)(ec, format))
  }

  def upsertFromPublisher[T](values: Publisher[(String, T)], settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Source[T, NotUsed] = {
    upsertFromSource[T, NotUsed](Source.fromPublisher(values), settings)(ec, format)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def searchFlow[T]()(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Flow[QueryLike, T, NotUsed] = {
    Flow[QueryLike].flatMapConcat(query => search[T](query)(ec, mat, reader).asSource)
  }

  def searchFromSource[T, Mat](query: Source[QueryLike, Mat])(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Source[T, Mat] = {
    query.via(searchFlow[T]()(ec, mat, reader))
  }

  def searchFromPublisher[T](query: Publisher[QueryLike])(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Source[T, NotUsed] = {
    searchFromSource[T, NotUsed](Source.fromPublisher(query))(ec, mat, reader)
  }

  def searchViewFlow[T]()(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Flow[ViewQuery, ViewRow[T], NotUsed] = {
    Flow[ViewQuery].flatMapConcat(query => searchView[T](query)(ec, mat, reader).asSource)
  }

  def searchViewFromSource[T, Mat](query: Source[ViewQuery, Mat])(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Source[ViewRow[T], Mat] = {
    query.via(searchViewFlow[T]()(ec, mat, reader))
  }

  def searchViewFromPublisher[T](query: Publisher[ViewQuery])(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Source[ViewRow[T], NotUsed] = {
    searchViewFromSource[T, NotUsed](Source.fromPublisher(query))(ec, mat, reader)
  }

  def searchSpatialFlow[T]()(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Flow[SpatialQuery, SpatialViewRow[T], NotUsed] = {
    Flow[SpatialQuery].flatMapConcat(query => searchSpatial[T](query)(ec, mat, reader).asSource)
  }

  def searchSpatialFromSource[T, Mat](query: Source[SpatialQuery, Mat])(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Source[SpatialViewRow[T], Mat] = {
    query.via(searchSpatialFlow[T]()(ec, mat, reader))
  }

  def searchSpatialFromPublisher[T](query: Publisher[SpatialQuery])(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): Source[SpatialViewRow[T], NotUsed] = {
    searchSpatialFromSource[T, NotUsed](Source.fromPublisher(query))(ec, mat, reader)
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
    _futureBucket.flatMap { bucket =>
      bucket.counter(key, delta, initialValue, settings.persistTo, settings.replicateTo).asFuture.map(_.content())
    }
  }

  def counterValue(key: String, settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext): Future[Long] = {
    _futureBucket.flatMap { bucket =>
      bucket.get(JsonLongDocument.create(key)).asFuture.filter(_ != null).map(_.content())
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def close()(implicit ec: ExecutionContext): Future[Boolean] = {
    onStop()
    _futureBucket.flatMap(_.close().asFuture.map(_.booleanValue())).andThen {
      case _ => _cluster.disconnect()
    }
  }

  def cluster(implicit ec: ExecutionContext): Future[ClusterFacade] = {
    _futureBucket.flatMap(b => b.core().asFuture)
  }

  def environment(implicit ec: ExecutionContext): Future[CouchbaseEnvironment] = {
    _futureBucket.map(b => b.environment())
  }

  def repository(implicit ec: ExecutionContext): Future[AsyncRepository] = {
    _futureBucket.flatMap(b => b.repository().asFuture)
  }

  def manager(implicit ec: ExecutionContext): Future[AsyncBucketManager] = {
    _futureBucket.flatMap(b => b.bucketManager().asFuture)
  }

  def withCluster[T](f: ClusterFacade => Observable[T])(implicit ec: ExecutionContext): Future[T] = {
    cluster(ec).flatMap(c => f(c).asFuture)
  }

  def withRepository[T](f: AsyncRepository => Observable[T])(implicit ec: ExecutionContext): Future[T] = {
    repository(ec).flatMap(r => f(r).asFuture)
  }

  def withEnvironment[T](f: CouchbaseEnvironment => T)(implicit ec: ExecutionContext): Future[T] = {
    environment(ec).map(e => f(e))
  }

  def withManager[T](f: AsyncBucketManager => Observable[T])(implicit ec: ExecutionContext): Future[T] = {
    manager(ec).flatMap(m => f(m).asFuture)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def exists(key: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    _futureBucket.flatMap { bucket =>
      bucket.exists(key).asFuture.map(_.booleanValue())
    }
  }

  def remove(key: String, settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext): Future[Boolean] = {
    _futureBucket.flatMap { bucket =>
      bucket.remove(key, settings.persistTo, settings.replicateTo).asFuture.map(_ != null)
    }
  }

  def insert[T](key: String, slug: T, settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Future[T] = {
    _futureBucket.flatMap { bucket =>
      bucket.insert(
        RawJsonDocument.create(
          key,
          settings.expiration.toMillis.toInt,
          Json.stringify(format.writes(slug))
        ),
        settings.persistTo,
        settings.replicateTo
      ).asFuture.map(doc => {
        println(doc.content())
        format.reads(Json.parse(doc.content())).get
      })
    }
  }

  def upsert[T](key: String, slug: T, settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext, format: Format[T]): Future[T] = {
    _futureBucket.flatMap { bucket =>
      bucket.upsert(
        RawJsonDocument.create(
          key,
          settings.expiration.toMillis.toInt,
          Json.stringify(format.writes(slug))
        ),
        settings.persistTo,
        settings.replicateTo
      ).asFuture.map(doc => format.reads(Json.parse(doc.content())).get)
    }
  }

  def get[T](key: String)(implicit ec: ExecutionContext, reader: Reads[T]): Future[Option[T]] = {
    _futureBucket.flatMap(b => b.get(RawJsonDocument.create(key)).asFuture)
      .filter(_ != null)
      .map(doc => Json.parse(doc.content()))
      .map(jsDoc => reader.reads(jsDoc).asOpt).recoverWith {
        case ObservableCompletedWithoutValue => Future.successful(None)
      }
  }

  def searchSpatial[T](query: SpatialQuery)(implicit ec: ExecutionContext, mat: Materializer, reader: Reads[T]): QueryResult[SpatialViewRow[T], NotUsed] = {
    SimpleQueryResult(() => {
      val obs: Observable[SpatialViewRow[T]] = _asyncBucket.query(query.query)
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
      val obs: Observable[ViewRow[T]] = _asyncBucket.query(query.query)
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
          _asyncBucket.query(com.couchbase.client.java.query.N1qlQuery.simple(n1ql))
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
          _asyncBucket.query(com.couchbase.client.java.query.N1qlQuery.parameterized(n1ql, params))
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
