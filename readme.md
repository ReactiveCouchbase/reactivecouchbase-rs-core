# ReactiveCouchbase v2

Yes, it's happening !!! with ReactiveStreams support ;-)

## If you want to try it

```sh
git clone https://github.com/ReactiveCouchbase/reactivecouchbase-rs-core.git
cd reactivecouchbase-rs-core
sbt ';clean;compile;publish-local'
```

then in your project add the following dependency

```
libraryDependencies += "org.reactivecouchbase" % "reactivecouchbase-core" % "2.0.0-SNAPSHOT"
```

and you're ready to go

## A small example

```scala
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import org.reactivecouchbase.scaladsl.{N1qlQuery, ReactiveCouchbase}
import play.api.libs.json.Json
import akka.stream.scaladsl.Sink
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

object ReactiveCouchbaseTest extends App {

  implicit val system = ActorSystem("ReactiveCouchbaseSystem")
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

  val (_, _, docs) = for {
    _        <- bucket.insert("key1", Json.obj("message" -> "Hello World", "type" -> "doc"))
    doc      <- bucket.get("key1")
    exists   <- bucket.exists("key1")
    docs     <- bucket.search(N1qlQuery("select message from default where type = $type").on(Json.obj("type" -> "doc"))).asSeq
    messages <- bucket.search(N1qlQuery("select message from default where type = 'doc'"))
                  .asSource.map(doc => (doc \ "message").as[String].toUpperCase)
                  .runWith(Sink.seq[String])
    _        <- driver.terminate()
  } yield (doc, exists, docs)

  println(s"found $docs")

}
```

## What about the Play Framework plugin

I don't think you actually need a plugin, if you want to use it from Play Framework, you can define a service to access your buckets like the following :


```scala
import javax.inject
import play.api.inject.ApplicationLifecycle
import play.api.Configuration

@Singleton
class Couchbase @Inject()(configuration: Configuration, lifecycle: ApplicationLifecycle) {

  private val driver = ReactiveCouchbase(configuration.underlying.getConfig("reactivecouchbase"))

  def bucket(name: String): Bucket = driver.bucket(name)

  lifecycle.addStopHook { () =>
    driver.terminate()
  }
}
```

so you can define a controller like the following

```scala
@Singleton
class ApiController @Inject()(couchbase: Couchbase)(implicit ec: ExecutionContext, materializer: Materializer) extends Controller {

  def eventsBucket = couchbase.bucket("events")

  def events(from: Option[String] = None) = Action {

    val date = from.map(DateTime.parse).getOrElse(DateTime.now())
    val source = eventsBucket.search(N1qlQuery("select id, payload, date, params from events where date > $date")
        .on(Json.obj("date" -> date.toString("dd-MM-yyyy")))
        .asSource
        .map(Json.stringify)
    Ok.chunked(source.intersperse("[", ",", "]")).as("application/json")
  }
}
``