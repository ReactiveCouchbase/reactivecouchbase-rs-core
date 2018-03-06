name := "reactivecouchbase-rs-core"
organization := "org.reactivecouchbase"
version := "1.1.1-SNAPSHOT"
scalaVersion := "2.12.4"
crossScalaVersions := Seq(scalaVersion.value, "2.11.11")

val circeVersion = "0.9.1"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.3",
  "com.couchbase.client" % "java-client" % "2.5.5",
  "com.typesafe.play" %% "play-json" % "2.6.9",
  "com.typesafe.akka" %% "akka-actor" % "2.5.11",
  "com.typesafe.akka" %% "akka-stream" % "2.5.11",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-java8" % circeVersion,
  "io.reactivex" % "rxjava-reactive-streams" % "1.2.1",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

val localPublishRepo: String = "./repository"

publishTo := {
  version.value match {
    case v if v.trim.endsWith("SNAPSHOT") =>
      Some(
        Resolver.file("snapshots", new File(localPublishRepo + "/snapshots")))
    case _ =>
      Some(Resolver.file("releases", new File(localPublishRepo + "/releases")))
  }
}

publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ =>
  false
}
pomExtra :=
  <url>https://github.com/ReactiveCouchbase/reactivecouchbase-rs-core</url>
  <licenses>
    <license>
      <name>Apache 2</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:ReactiveCouchbase/reactivecouchbase-rs-core.git</url>
    <connection>scm:git:git@github.com:ReactiveCouchbase/reactivecouchbase-rs-core.git</connection>
  </scm>
  <developers>
    <developer>
      <id>mathieu.ancelin</id>
      <name>Mathieu ANCELIN</name>
      <url>https://github.com/mathieuancelin</url>
    </developer>
  </developers>
