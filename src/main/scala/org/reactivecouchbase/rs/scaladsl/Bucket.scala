package org.reactivecouchbase.rs.scaladsl

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.actor.Cancellable
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import com.couchbase.client.core.ClusterFacade
import com.couchbase.client.java.{CouchbaseCluster, ReplicaMode}
import com.couchbase.client.java.bucket.AsyncBucketManager
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{JsonDocument, JsonLongDocument, RawJsonDocument}
import com.couchbase.client.java.env.{CouchbaseEnvironment, DefaultCouchbaseEnvironment}
import com.couchbase.client.java.repository.AsyncRepository
import com.couchbase.client.java.error.subdoc.PathNotFoundException
import com.couchbase.client.java.subdoc._
import com.couchbase.client.core.message.kv.subdoc.multi._
import com.typesafe.config.Config
import org.reactivecouchbase.rs.scaladsl.TypeUtils.EnvCustomizer
import org.reactivecouchbase.rs.scaladsl.json.{JsonError, JsonFormat, JsonReads, JsonSuccess, JsonWrites}
import org.reactivestreams.Publisher
import rx.{Observable, RxReactiveStreams}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.reflect._

case class ClusterAuth(username: String, password: String)

case class BucketConfig(name: String,
                        password: Option[String] = None, // For 4.x backwards compat
                        hosts: Seq[String],
                        env: EnvCustomizer,
                        defaultTimeout: Option[Duration],
                        clusterAuth: Option[ClusterAuth] = None //For 5.x RBAC requirements
                       )

object BucketConfig {
  import collection.JavaConversions._

  def apply(config: Config, env: EnvCustomizer, defaultTimeout: Option[Duration]): BucketConfig = {
    val name = Try(config.getString("name")).get
    val password = Try(config.getString("password")).toOption
    val hosts = Try(config.getStringList("hosts")).get.toIndexedSeq
    val auth = Try {
      val username = config.getConfig("authentication").getString("username")
      val password = config.getConfig("authentication").getString("password")
      ClusterAuth(username, password)
    }.toOption

    BucketConfig(name, password, hosts, env, defaultTimeout, auth)
  }
}

object Bucket {
  def apply(config: BucketConfig, onStop: () => Unit) = new Bucket(config, onStop)
}

class Bucket(config: BucketConfig, onStop: () => Unit) {

  private val defaultWriteSettings: WriteSettings = WriteSettings()

  private val env = config.env(DefaultCouchbaseEnvironment.builder()).build()

  private val _cluster: CouchbaseCluster = {
    val underlying = CouchbaseCluster.create(env, config.hosts: _*)
    config.clusterAuth.foreach(auth => underlying.authenticate(auth.username, auth.password))
    underlying
  }

  private val (_bucket, _asyncBucket, _bucketManager, _futureBucket) = {
    val _bucket = config.password
      .map(p => _cluster.openBucket(config.name, p))
      .getOrElse(_cluster.openBucket(config.name))
    val _bucketManager = _bucket.bucketManager()
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

  def tailSearchFlow[T](query: Long => QueryLike, extractor: (T, Long) => Long, from: Long = 0L, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Flow[NotUsed, T, NotUsed] = {
    val ref = new AtomicReference[Long](from)
    val last = new AtomicReference[T]()
    Flow[NotUsed].flatMapConcat { _ =>
      search[T](query(ref.get()), timeout)(ec, mat, reader)
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

  def tailViewSearchFlow[T](query: Long => ViewQuery, extractor: (ViewRow[T], Long) => Long, from: Long = 0L, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Flow[NotUsed, ViewRow[T], NotUsed] = {
    val ref = new AtomicReference[Long](from)
    val last = new AtomicReference[ViewRow[T]]()
    Flow[NotUsed].flatMapConcat { _ =>
      searchView[T](query(ref.get()), timeout)(ec, mat, reader)
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

  def tailSpatialSearchFlow[T](query: Long => SpatialQuery, extractor: (SpatialViewRow[T], Long) => Long, from: Long = 0L, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Flow[NotUsed, SpatialViewRow[T], NotUsed] = {
    val ref = new AtomicReference[Long](from)
    val last = new AtomicReference[SpatialViewRow[T]]()
    Flow[NotUsed].flatMapConcat { _ =>
      searchSpatial[T](query(ref.get()), timeout)(ec, mat, reader)
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

  def tailSearch[T](query: Long => QueryLike, extractor: (T, Long) => Long, from: Long = 0L, limit: Long = Long.MaxValue, every: FiniteDuration = FiniteDuration(200, TimeUnit.MILLISECONDS), timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Source[T, Cancellable] = {
    Source.tick(
      FiniteDuration(0, TimeUnit.MILLISECONDS),
      every,
      NotUsed
    ).via(tailSearchFlow[T](query, extractor, from, timeout)(ec, mat, reader)).take(limit)
  }

  def tailViewSearch[T](query: Long => ViewQuery, extractor: (ViewRow[T], Long) => Long, from: Long = 0L, limit: Long = Long.MaxValue, every: FiniteDuration = FiniteDuration(200, TimeUnit.MILLISECONDS), timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Source[ViewRow[T], Cancellable] = {
    Source.tick(
      FiniteDuration(0, TimeUnit.MILLISECONDS),
      every,
      NotUsed
    ).via(tailViewSearchFlow[T](query, extractor, from, timeout)(ec, mat, reader)).take(limit)
  }

  def tailSpatialSearch[T](query: Long => SpatialQuery, extractor: (SpatialViewRow[T], Long) => Long, from: Long = 0L, limit: Long = Long.MaxValue, every: FiniteDuration = FiniteDuration(200, TimeUnit.MILLISECONDS), timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Source[SpatialViewRow[T], Cancellable] = {
    Source.tick(
      FiniteDuration(0, TimeUnit.MILLISECONDS),
      every,
      NotUsed
    ).via(tailSpatialSearchFlow[T](query, extractor, from, timeout)(ec, mat, reader)).take(limit)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def getFlow[T](timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, reader: JsonReads[T]): Flow[String, T, NotUsed] = {
    Flow[String].flatMapConcat(k => Source.fromFuture(get[T](k, timeout)(ec, reader))).filter(_.isDefined).map(_.get)
  }

  def getFromSource[T, Mat](keys: Source[String, Mat], timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, reader: JsonReads[T]): Source[T, Mat] = {
    keys.via(getFlow[T](timeout)(ec, reader))
  }

  def getFromPublisher[T](keys: Publisher[String], timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, reader: JsonReads[T]): Source[T, NotUsed] = {
    getFromSource[T, NotUsed](Source.fromPublisher(keys), timeout)(ec, reader)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def removeFlow(settings: WriteSettings = defaultWriteSettings, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Flow[String, Boolean, NotUsed] = {
    Flow[String].flatMapConcat(k => Source.fromFuture(remove(k, settings, timeout)(ec)))
  }

  def removeFromSource[Mat](keys: Source[String, Mat], settings: WriteSettings = defaultWriteSettings, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Source[Boolean, Mat] = {
    keys.via(removeFlow(settings, timeout)(ec))
  }

  def removeFromPublisher(keys: Publisher[String], settings: WriteSettings = defaultWriteSettings, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Source[Boolean, NotUsed] = {
    removeFromSource[NotUsed](Source.fromPublisher(keys), settings, timeout)(ec)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def insertFlow[T](settings: WriteSettings = defaultWriteSettings, cas: Option[Long] = None, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, format: JsonFormat[T]): Flow[(String, T), T, NotUsed] = {
    Flow[(String, T)].flatMapConcat(tuple => Source.fromFuture(insert[T](tuple._1, tuple._2, settings, cas, timeout)(ec, format)))
  }

  def insertFromSource[T, Mat](values: Source[(String, T), Mat], settings: WriteSettings = defaultWriteSettings, cas: Option[Long] = None, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, format: JsonFormat[T]): Source[T, Mat] = {
    values.via(insertFlow[T](settings, cas, timeout)(ec, format))
  }

  def insertFromPublisher[T](values: Publisher[(String, T)], settings: WriteSettings = defaultWriteSettings, cas: Option[Long] = None, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, format: JsonFormat[T]): Source[T, NotUsed] = {
    insertFromSource[T, NotUsed](Source.fromPublisher(values), settings, cas, timeout)(ec, format)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def upsertFlow[T](settings: WriteSettings = defaultWriteSettings, cas: Option[Long] = None, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, format: JsonFormat[T]): Flow[(String, T), T, NotUsed] = {
    Flow[(String, T)].flatMapConcat(tuple => Source.fromFuture(upsert[T](tuple._1, tuple._2, settings)(ec, format)))
  }

  def upsertFromSource[T, Mat](values: Source[(String, T), Mat], settings: WriteSettings = defaultWriteSettings, cas: Option[Long] = None, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, format: JsonFormat[T]): Source[T, Mat] = {
    values.via(upsertFlow[T](settings, cas, timeout)(ec, format))
  }

  def upsertFromPublisher[T](values: Publisher[(String, T)], settings: WriteSettings = defaultWriteSettings, cas: Option[Long] = None, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, format: JsonFormat[T]): Source[T, NotUsed] = {
    upsertFromSource[T, NotUsed](Source.fromPublisher(values), settings, cas, timeout)(ec, format)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def searchFlow[T](timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Flow[QueryLike, T, NotUsed] = {
    Flow[QueryLike].flatMapConcat(query => search[T](query, timeout)(ec, mat, reader).asSource)
  }

  def searchFromSource[T, Mat](query: Source[QueryLike, Mat], timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Source[T, Mat] = {
    query.via(searchFlow[T](timeout)(ec, mat, reader))
  }

  def searchFromPublisher[T](query: Publisher[QueryLike], timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Source[T, NotUsed] = {
    searchFromSource[T, NotUsed](Source.fromPublisher(query), timeout)(ec, mat, reader)
  }

  def searchViewFlow[T](timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Flow[ViewQuery, ViewRow[T], NotUsed] = {
    Flow[ViewQuery].flatMapConcat(query => searchView[T](query)(ec, mat, reader).asSource)
  }

  def searchViewFromSource[T, Mat](query: Source[ViewQuery, Mat], timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Source[ViewRow[T], Mat] = {
    query.via(searchViewFlow[T](timeout)(ec, mat, reader))
  }

  def searchViewFromPublisher[T](query: Publisher[ViewQuery], timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Source[ViewRow[T], NotUsed] = {
    searchViewFromSource[T, NotUsed](Source.fromPublisher(query), timeout)(ec, mat, reader)
  }

  def searchSpatialFlow[T](timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Flow[SpatialQuery, SpatialViewRow[T], NotUsed] = {
    Flow[SpatialQuery].flatMapConcat(query => searchSpatial[T](query, timeout)(ec, mat, reader).asSource)
  }

  def searchSpatialFromSource[T, Mat](query: Source[SpatialQuery, Mat], timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Source[SpatialViewRow[T], Mat] = {
    query.via(searchSpatialFlow[T](timeout)(ec, mat, reader))
  }

  def searchSpatialFromPublisher[T](query: Publisher[SpatialQuery], timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): Source[SpatialViewRow[T], NotUsed] = {
    searchSpatialFromSource[T, NotUsed](Source.fromPublisher(query), timeout)(ec, mat, reader)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  object maps {
    def put[T](key: String, entry: String, slug: T, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
      _futureBucket.flatMap { bucket =>
        bucket.mapAdd(key, entry, slug).maybeTimeout(timeout).asFuture.map(_.booleanValue)
      }
    }
    def get[T](key: String, entry: String, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, classTag: ClassTag[T]): Future[Option[T]] = {
      _futureBucket.flatMap { bucket =>
        bucket.mapGet(key, entry, classTag.runtimeClass.asInstanceOf[Class[T]]).maybeTimeout(timeout).asFuture.map(Option.apply).recover {
          case e: PathNotFoundException => None
        }
      }
    }
    def remove[T](key: String, entry: String, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
      _futureBucket.flatMap { bucket =>
        bucket.mapRemove(key, entry).maybeTimeout(timeout).asFuture.map(_.booleanValue)
      }
    }
    def size(key: String, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Int] = {
      _futureBucket.flatMap { bucket =>
        bucket.mapSize(key).maybeTimeout(timeout).asFuture.map(_.toInt)
      }
    }
  }

  object queues {
    def push[T](key: String, slug: T, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
      _futureBucket.flatMap { bucket =>
        bucket.queuePush(key, slug).maybeTimeout(timeout).asFuture.map(_.booleanValue)
      }
    }
    def pop[T](key: String, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, classTag: ClassTag[T]): Future[Option[T]] = {
      _futureBucket.flatMap { bucket =>
        bucket.queuePop(key, classTag.runtimeClass.asInstanceOf[Class[T]]).maybeTimeout(timeout).asFuture.map(Option.apply)
      }
    }
    def size(key: String, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Int] = {
      _futureBucket.flatMap { bucket =>
        bucket.queueSize(key).maybeTimeout(timeout).asFuture.map(_.toInt)
      }
    }
  }

  object lists {
    def set[T](key: String, index: Int, slug: T, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
      _futureBucket.flatMap { bucket =>
        bucket.listSet(key, index, slug).maybeTimeout(timeout).asFuture.map(_.booleanValue)
      }
    }
    def append[T](key: String, slug: T, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
      _futureBucket.flatMap { bucket =>
        bucket.listAppend(key, slug).maybeTimeout(timeout).asFuture.map(_.booleanValue)
      }
    }
    def prepend[T](key: String, slug: T, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
      _futureBucket.flatMap { bucket =>
        bucket.listPrepend(key, slug).maybeTimeout(timeout).asFuture.map(_.booleanValue)
      }
    }
    def get[T](key: String, index: Int, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, classTag: ClassTag[T]): Future[Option[T]] = {
      _futureBucket.flatMap { bucket =>
        bucket.listGet(key, index, classTag.runtimeClass.asInstanceOf[Class[T]]).maybeTimeout(timeout).asFuture.map(Option.apply).recover {
          case e: PathNotFoundException => None
        }
      }
    }
    def remove[T](key: String, index: Int, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
      _futureBucket.flatMap { bucket =>
        bucket.listRemove(key, index).maybeTimeout(timeout).asFuture.map(_.booleanValue)
      }
    }
    def size(key: String, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Int] = {
      _futureBucket.flatMap { bucket =>
        bucket.listSize(key).maybeTimeout(timeout).asFuture.map(_.toInt)
      }
    }
  }

  object sets {
    def add[T](key: String, slug: T, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
      _futureBucket.flatMap { bucket =>
        bucket.setAdd(key, slug).maybeTimeout(timeout).asFuture.map(_.booleanValue)
      }
    }
    def remove[T](key: String, slug: T, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Option[T]] = {
      _futureBucket.flatMap { bucket =>
        bucket.setRemove(key, slug).maybeTimeout(timeout).asFuture.map(Option.apply).recover {
          case e: PathNotFoundException => None
        }
      }
    }
    def size(key: String, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Int] = {
      _futureBucket.flatMap { bucket =>
        bucket.setSize(key).maybeTimeout(timeout).asFuture.map(_.toInt)
      }
    }
    def contains[T](key: String, slug: T, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
      _futureBucket.flatMap { bucket =>
        bucket.setContains(key, slug).maybeTimeout(timeout).asFuture.map(_.booleanValue)
      }
    }
  }

  def unlock(key: String, cas: Long, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
    _futureBucket.flatMap { bucket =>
      bucket.unlock(key, cas).maybeTimeout(timeout).asFuture.map(_.booleanValue)
    }
  }
  def touch(key: String, expiry: Duration, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
    _futureBucket.flatMap { bucket =>
      bucket.touch(key, expiry.asCouchbaseExpiry).maybeTimeout(timeout).asFuture.map(_.booleanValue)
    }
  }
  def getAndLock[T](key: String, lockTime: Duration, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, format: JsonFormat[T], classTag: ClassTag[T]): Future[Option[T]] = {
    _futureBucket.flatMap { bucket =>
      bucket.getAndLock(key, lockTime.toSeconds.toInt, classOf[RawJsonDocument]).maybeTimeout(timeout).asFuture
        .filter(_ != null)
        .map(doc => ByteString(doc.content()))
        .map(jsDoc => format.reads(jsDoc).asOpt).recoverWith {
          case ObservableCompletedWithoutValue => Future.successful(None)
        }
    }
  }

  // still crappy, will add proper support later
  def mutateIn(key: String, timeout: Option[Duration] = config.defaultTimeout)(f: AsyncMutateInBuilder => rx.Observable[DocumentFragment[Mutation]])(implicit ec: ExecutionContext): Future[DocumentFragment[Mutation]] = {
    _futureBucket.flatMap { bucket =>
        f(bucket.mutateIn(key)).maybeTimeout(timeout).asFuture
      }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def incr(key: String, settings: WriteSettings = defaultWriteSettings, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Long] = counter(key, 1L, 0L, settings, timeout)(ec)

  def decr(key: String, settings: WriteSettings = defaultWriteSettings, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Long] = counter(key, -1L, 0L, settings, timeout)(ec)

  def counter(key: String, delta: Long = 0L, initialValue: Long = 0L, settings: WriteSettings = defaultWriteSettings, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Long] = {
    _futureBucket.flatMap { bucket =>
      bucket.counter(key, delta, initialValue, settings.persistTo, settings.replicateTo).maybeTimeout(timeout).asFuture.map(_.content())
    }
  }

  def counterValue(key: String, settings: WriteSettings = defaultWriteSettings, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Long] = {
    _futureBucket.flatMap { bucket =>
      bucket.get(JsonLongDocument.create(key)).maybeTimeout(timeout).asFuture.filter(_ != null).map(_.content())
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def invalidateQueryCache(timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Int] = {
    _futureBucket.flatMap { bucket =>
      bucket.invalidateQueryCache().maybeTimeout(timeout).asFuture.map(_.toInt)
    }
  }

  def close(timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
    onStop()
    _futureBucket.flatMap(_.close().maybeTimeout(timeout).asFuture.map(_.booleanValue())).andThen {
      case _ => _cluster.disconnect()
    } flatMap( _ => env.shutdownAsync().maybeTimeout(timeout).asFuture.map(_.booleanValue()))
  }

  def cluster(timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[ClusterFacade] = {
    _futureBucket.flatMap(b => b.core().maybeTimeout(timeout).asFuture)
  }

  def environment(implicit ec: ExecutionContext): Future[CouchbaseEnvironment] = {
    _futureBucket.map(b => b.environment())
  }

  def repository(timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[AsyncRepository] = {
    _futureBucket.flatMap(b => b.repository().maybeTimeout(timeout).asFuture)
  }

  def manager(timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[AsyncBucketManager] = {
    _futureBucket.flatMap(b => b.bucketManager().maybeTimeout(timeout).asFuture)
  }

  def withCluster[T](f: ClusterFacade => Observable[T], timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[T] = {
    cluster(timeout)(ec).flatMap(c => f(c).maybeTimeout(timeout).asFuture)
  }

  def withRepository[T](f: AsyncRepository => Observable[T], timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[T] = {
    repository(timeout)(ec).flatMap(r => f(r).maybeTimeout(timeout).asFuture)
  }

  def withEnvironment[T](f: CouchbaseEnvironment => T)(implicit ec: ExecutionContext): Future[T] = {
    environment(ec).map(e => f(e))
  }

  def withManager[T](f: AsyncBucketManager => Observable[T], timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[T] = {
    manager(timeout)(ec).flatMap(m => f(m).maybeTimeout(timeout).asFuture)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  
  def replace[T](key: String, slug: T, settings: WriteSettings = defaultWriteSettings, cas: Option[Long] = None, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, format: JsonFormat[T]): Future[T] = {
    _futureBucket.flatMap { bucket =>
      val doc = cas match {
        case None => RawJsonDocument.create(
          key,
          settings.expiration.asCouchbaseExpiry,
          format.writes(slug).utf8String
        )
        case Some(casValue) => RawJsonDocument.create(
          key,
          settings.expiration.asCouchbaseExpiry,
          format.writes(slug).utf8String,
          casValue
        )
      }
      bucket.replace(
        doc,
        settings.persistTo,
        settings.replicateTo
      ).maybeTimeout(timeout).asFuture.map(doc => {
        format.reads(ByteString(doc.content())).get
      })
    }
  }

  def exists(key: String, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
    _futureBucket.flatMap { bucket =>
      bucket.exists(key).maybeTimeout(timeout).asFuture.map(_.booleanValue())
    }
  }

  def remove(key: String, settings: WriteSettings = defaultWriteSettings, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext): Future[Boolean] = {
    _futureBucket.flatMap { bucket =>
      bucket.remove(key, settings.persistTo, settings.replicateTo).maybeTimeout(timeout).asFuture.map(_ != null)
    }
  }

  def insert[T](key: String, slug: T, settings: WriteSettings = defaultWriteSettings, cas: Option[Long] = None, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, format: JsonFormat[T]): Future[T] = {
    _futureBucket.flatMap { bucket =>
      val doc = cas match {
        case None => RawJsonDocument.create(
          key,
          settings.expiration.asCouchbaseExpiry,
          format.writes(slug).utf8String
        )
        case Some(casValue) => RawJsonDocument.create(
          key,
          settings.expiration.asCouchbaseExpiry,
          format.writes(slug).utf8String,
          casValue
        )
      }
      bucket.insert(
        doc,
        settings.persistTo,
        settings.replicateTo
      ).maybeTimeout(timeout).asFuture.map(doc => {
        format.reads(ByteString(doc.content())).get
      })
    }
  }

  def upsert[T](key: String, slug: T, settings: WriteSettings = defaultWriteSettings, cas: Option[Long] = None, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, format: JsonFormat[T]): Future[T] = {
    _futureBucket.flatMap { bucket =>
      val doc = cas match {
        case None => RawJsonDocument.create(
          key,
          settings.expiration.asCouchbaseExpiry,
          format.writes(slug).utf8String
        )
        case Some(casValue) => RawJsonDocument.create(
          key,
          settings.expiration.asCouchbaseExpiry,
          format.writes(slug).utf8String,
          casValue
        )
      }
      bucket.upsert(
        doc,
        settings.persistTo,
        settings.replicateTo
      ).maybeTimeout(timeout).asFuture.map(doc => format.reads(ByteString(doc.content())).get)
    }
  }

  def get[T](key: String, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, reader: JsonReads[T]): Future[Option[T]] = {
    _futureBucket.flatMap(b => b.get(RawJsonDocument.create(key)).maybeTimeout(timeout).asFuture)
      .filter(_ != null)
      .map(doc => ByteString(doc.content()))
      .map(jsDoc => reader.reads(jsDoc).asOpt).recoverWith {
        case ObservableCompletedWithoutValue => Future.successful(None)
      }
  }

  def getFromReplica[T](key: String, replicaMode: ReplicaMode, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, reader: JsonReads[T]): Future[Option[T]] = {
    _futureBucket.flatMap(b => b.getFromReplica(RawJsonDocument.create(key), replicaMode).maybeTimeout(timeout).asFuture)
      .filter(_ != null)
      .map(doc => ByteString(doc.content()))
      .map(jsDoc => reader.reads(jsDoc).asOpt).recoverWith {
      case ObservableCompletedWithoutValue => Future.successful(None)
    }
  }

  def getWithCAS[T](key: String, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, reader: JsonReads[T]): Future[Option[(T, Long)]] = {
    _futureBucket.flatMap(b => b.get(RawJsonDocument.create(key)).maybeTimeout(timeout).asFuture)
      .filter(_ != null)
      .map(doc => (ByteString(doc.content()), doc.cas))
      .map {
        case (jsDoc, cas) => reader.reads(jsDoc).asOpt.map(v => (v, cas))
      } recoverWith {
        case ObservableCompletedWithoutValue => Future.successful(None)
      }
  }

  def getAndTouch[T](key: String, expiration: Duration, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, reader: JsonReads[T]): Future[Option[T]] = {
    _futureBucket.flatMap(b => b.getAndTouch(key, expiration.asCouchbaseExpiry, classOf[RawJsonDocument]).maybeTimeout(timeout).asFuture)
      .filter(_ != null)
      .map(doc => ByteString(doc.content()))
      .map(jsDoc => reader.reads(jsDoc).asOpt).recoverWith {
        case ObservableCompletedWithoutValue => Future.successful(None)
      }
  }

  def searchSpatial[T](query: SpatialQuery, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): QueryResult[SpatialViewRow[T], NotUsed] = {
    SimpleQueryResult(() => {
      val obs: Observable[SpatialViewRow[T]] = _asyncBucket.query(query.query)
        .maybeTimeout(timeout)
        .flatMap(RxUtils.func1(_.rows()))
        .map(RxUtils.func1 { row => SpatialViewRow[T](row, reader) })
      Source.fromPublisher[SpatialViewRow[T]](RxReactiveStreams.toPublisher[SpatialViewRow[T]](obs))
    })
  }

  def searchView[T](query: ViewQuery, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): QueryResult[ViewRow[T], NotUsed] = {
    SimpleQueryResult(() => {
      val obs: Observable[ViewRow[T]] = _asyncBucket.query(query.query)
        .maybeTimeout(timeout)
        .flatMap(RxUtils.func1(_.rows()))
        .map(RxUtils.func1 { row => ViewRow(row, reader) })
      Source.fromPublisher[ViewRow[T]](RxReactiveStreams.toPublisher[ViewRow[T]](obs))
    })
  }

  def search[T](query: QueryLike, timeout: Option[Duration] = config.defaultTimeout)(implicit ec: ExecutionContext, mat: Materializer, reader: JsonReads[T]): QueryResult[T, NotUsed] = {
    SimpleQueryResult(() => {
      val obs: Observable[T] = query match {
        case N1qlQuery(n1ql, args) if args.isEmpty => {
          _asyncBucket.query(com.couchbase.client.java.query.N1qlQuery.simple(n1ql))
            .maybeTimeout(timeout)
            .flatMap(RxUtils.func1(_.rows()))
            .map(RxUtils.func1 { t =>
              reader.reads(ByteString(t.byteValue())) match {
                case JsonSuccess(s) => s
                case JsonError(e) => throw new RuntimeException(s"Error while parsing document : $e") // TODO : better error
              }
            })
        }
        case N1qlQuery(n1ql, args) if args.nonEmpty => {
          val params: JsonObject = args.toJsonObject
          _asyncBucket.query(com.couchbase.client.java.query.N1qlQuery.parameterized(n1ql, params))
            .maybeTimeout(timeout)
            .flatMap(RxUtils.func1(_.rows()))
            .map(RxUtils.func1 { t =>
              reader.reads(ByteString(t.byteValue())) match {
                case JsonSuccess(s) => s
                case JsonError(e) => throw new RuntimeException(s"Error while parsing document : $e") // TODO : better error
              }
            })
        }
        case _ => Observable.error(new UnsupportedOperationException("Query not supported !"))
      }
      Source.fromPublisher[T](RxReactiveStreams.toPublisher[T](obs))
    })
  }
}
