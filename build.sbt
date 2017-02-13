name := """reactivecouchbase-core"""
organization := "org.reactivecouchbase"
version := "2.0.0-SNAPSHOT"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.1",
  "com.couchbase.client" % "java-client" % "2.4.1",
  "com.typesafe.play" %% "play-json" % "2.4.10",
  "com.typesafe.akka" %% "akka-actor" % "2.4.11.1",
  "com.typesafe.akka" %% "akka-stream" % "2.4.11.1",
  "io.reactivex" % "rxjava-reactive-streams" % "1.2.1",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
) 

val local: Project.Initialize[Option[sbt.Resolver]] = version { (version: String) =>
  val localPublishRepo = "./repository"
  if(version.trim.endsWith("SNAPSHOT"))
    Some(Resolver.file("snapshots", new File(localPublishRepo + "/snapshots")))
  else Some(Resolver.file("releases", new File(localPublishRepo + "/releases")))
}

publishTo <<= local
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }
pomExtra := (
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
)