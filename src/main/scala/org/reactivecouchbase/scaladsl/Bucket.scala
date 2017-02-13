package org.reactivecouchbase.scaladsl

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.bucket.AsyncBucketManager
import com.couchbase.client.java.document.RawJsonDocument
import com.typesafe.config.Config
import org.reactivecouchbase.scaladsl.Implicits._
import play.api.libs.json._
import rx.{Observable, RxReactiveStreams}

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

  private val defaultReads: Reads[JsValue] = Reads.apply(jsv => JsSuccess(jsv))
  private val defaultWrites: Writes[JsValue] = Writes.apply(jsv => jsv)
  private val defaultWriteSettings: WriteSettings = WriteSettings()

  // TODO : map clusterManager

  // TODO : pass complex settings
  private val cluster: CouchbaseCluster = CouchbaseCluster.create(config.hosts:_*)

  // TODO : implements in a non blocking fashion
  private val (bucket, asyncBucket, bucketManager, futureBucket) = {
    val _bucket = config.password
      .map(p => cluster.openBucket(config.name, p))
      .getOrElse(cluster.openBucket(config.name))
    // TODO : map bucketManager
    val _bucketManager = _bucket.bucketManager()
    // TODO : avoid index creation
    _bucketManager.async().createN1qlPrimaryIndex(true, false)
    (_bucket, _bucket.async(), _bucketManager, Future.successful(_bucket.async()))
  }

  // TODO : implement tailable query
  // TODO : implement other searches
  // TODO : implement management (design doc, etc ...)

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def getStream[T](keys: Source[String, _], reader: Reads[T] = defaultReads)(implicit ec: ExecutionContext): Source[T, _] = {
    keys.flatMapConcat(k => Source.fromFuture(get[T](k, reader)(ec))).filter(_.isDefined).map(_.get)
  }

  def removeStream(keys: Source[String, _], settings: WriteSettings = defaultWriteSettings)(implicit ec: ExecutionContext): Source[Boolean, _] = {
    keys.flatMapConcat(k => Source.fromFuture(remove(k, settings)(ec)))
  }

  def insertStream[T](values: Source[(String, T), _], settings: WriteSettings = defaultWriteSettings, writer: Writes[T] = defaultWrites)(implicit ec: ExecutionContext): Source[JsValue, _] = {
    values.flatMapConcat(tuple => Source.fromFuture(insert[T](tuple._1, tuple._2, settings, writer)(ec)))
  }

  def upsertStream[T](values: Source[(String, T), _], settings: WriteSettings = defaultWriteSettings, writer: Writes[T] = defaultWrites)(implicit ec: ExecutionContext): Source[JsValue, _] = {
    values.flatMapConcat(tuple => Source.fromFuture(upsert[T](tuple._1, tuple._2, settings, writer)(ec)))
  }

  def searchStream[T](query: QueryLike, reader: Reads[T] = defaultReads)(implicit ec: ExecutionContext, materializer: Materializer): Source[T, _] = {
    search[T](query, reader)(ec, materializer).asSource
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // TODO : maps operations
  // TODO : sets operations
  // TODO : lists operations
  // TODO : queues operations
  // TODO : getAndTouch
  // TODO : getAndLock
  // TODO : counter operations

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
      .map(jsDoc => reader.reads(jsDoc).asOpt).recoverWith {
        case ObservableCompletedWithoutValue(_, _) => Future.successful(None)
      }
  }

  def searchSpatial[T](query: SpatialQuery, reader: Reads[T] = defaultReads)(implicit ec: ExecutionContext, materializer: Materializer): QueryResult[SpatialViewRow[T]] = {
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

  def searchView[T](query: ViewQuery, reader: Reads[T] = defaultReads)(implicit ec: ExecutionContext, materializer: Materializer): QueryResult[ViewRow[T]] = {
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

  def search[T](query: QueryLike, reader: Reads[T] = defaultReads)(implicit ec: ExecutionContext, materializer: Materializer): QueryResult[T] = {
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
