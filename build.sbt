import sbt.Keys.{organization, scalacOptions}
import sbtrelease.ReleaseStateTransformations._

val githubRepo = "ReactiveCouchbase/reactivecouchbase-rs-core"
val akkaVersion = "2.5.11"    
val circeVersion = "0.9.1"    
val disabledPlugins = if (sys.env.get("TRAVIS_TAG").filterNot(_.isEmpty).isDefined) {
  Seq.empty
} else {
  Seq(BintrayPlugin)
}

lazy val reactivecouchbase = (project in file("."))
  .disablePlugins(disabledPlugins: _*)
  .settings(
    name := "reactivecouchbase-rs-core",
    organization := "org.reactivecouchbase",
    version := "1.2.0-SNAPSHOT",
    scalaVersion := "2.12.4",
    crossScalaVersions := Seq(scalaVersion.value, "2.11.11"),
    libraryDependencies ++= Seq(
      "com.typesafe"         % "config"                  % "1.3.3",
      "com.couchbase.client" % "java-client"             % "2.5.5",
      "com.typesafe.play"    %% "play-json"              % "2.6.9",
      "com.typesafe.akka"    %% "akka-actor"             % akkaVersion,
      "com.typesafe.akka"    %% "akka-stream"            % akkaVersion,
      "io.circe"             %% "circe-core"             % circeVersion,
      "io.circe"             %% "circe-generic"          % circeVersion,
      "io.circe"             %% "circe-parser"           % circeVersion,
      "io.circe"             %% "circe-java8"            % circeVersion,
      "io.reactivex"         % "rxjava-reactive-streams" % "1.2.1",
      "org.scalatest"        %% "scalatest"              % "3.0.5" % "test"
    ),
    sources in (Compile, doc) := Seq.empty,
    publishArtifact in (Compile, packageDoc) := false,
    resolvers ++= Seq(
      Resolver.jcenterRepo,
      Resolver.bintrayRepo("larousso", "maven")
    ),
    scalafmtVersion in ThisBuild := "1.2.0"
  )
  .settings(publishSettings: _*)

lazy val publishCommonsSettings = Seq(
  homepage := Some(url(s"https://github.com/$githubRepo")),
  startYear := Some(2017),
  licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
  scmInfo := Some(
    ScmInfo(
      url(s"https://github.com/$githubRepo"),
      s"scm:git:https://github.com/$githubRepo.git",
      Some(s"scm:git:git@github.com:$githubRepo.git")
    )
  ),
  developers := List(
    Developer("mathieuancelin", "Mathieu ANCELIN", "", url("https://github.com/mathieuancelin"))
  ),
  releaseCrossBuild := true,
  publishMavenStyle := true,
  publishArtifact in Test := false,
  bintrayVcsUrl := Some(s"scm:git:git@github.com:$githubRepo.git")
)

lazy val publishSettings =
  if (sys.env.get("TRAVIS_TAG").filterNot(_.isEmpty).isDefined) {
    publishCommonsSettings ++ Seq(
      bintrayOrganization := Some("mathieuancelin"),
      bintrayRepository   := "reactivecouchbase-maven",
      pomIncludeRepository := { _ =>
        false
      }
    )
  } else {
    publishCommonsSettings
  }
