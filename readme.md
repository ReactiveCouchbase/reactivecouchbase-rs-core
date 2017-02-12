# ReactiveCouchbase ReactiveStreams

Yes, it's happening !!!

## If you want to try it

```sh
git clone https://github.com/ReactiveCouchbase/reactivecouchbase-rs-core.git
cd reactivecouchbase-rs-core
sbt
;clean;compile;publish-local
```

then in your project add the following dependency

```
libraryDependencies += "org.reactivecouchbase" % "reactivecouchbase-rs-core" % "1.0.0-SNAPSHOT"
```

and you're ready to go

```scala
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import org.reactivecouchbase.scaladsl.{N1qlQuery, ReactiveCouchbase}
import play.api.libs.json.Json
import akka.stream.scaladsl.Sink

object ReactiveCouchbaseTest extends App {

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
    _        <- bucket.close()
  } yield (doc, exists, docs)

  println(s"found $docs")

}
```