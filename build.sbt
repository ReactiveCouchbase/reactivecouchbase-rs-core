import sbt.Keys.{organization, scalacOptions}
import sbtrelease.ReleaseStateTransformations._

lazy val githubRepo   = "ReactiveCouchbase/reactivecouchbase-rs-core"
lazy val akkaVersion  = "2.5.12"
lazy val circeVersion = "0.9.3"
lazy val disabledPlugins = if (sys.env.get("TRAVIS_TAG").filterNot(_.isEmpty).isDefined) {
  Seq.empty
} else {
  Seq(BintrayPlugin)
}

lazy val reactivecouchbase = (project in file("."))
  .enablePlugins(GitVersioning, GitBranchPrompt)
  .disablePlugins(disabledPlugins: _*)
  .settings(
    name := "reactivecouchbase-rs-core",
    organization := "org.reactivecouchbase",
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
      Resolver.bintrayRepo("mathieuancelin", "reactivecouchbase-maven")
    ),
    scalafmtVersion in ThisBuild := "1.2.0",
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies, // : ReleaseStep
      inquireVersions, // : ReleaseStep
      runClean, // : ReleaseStep
      setReleaseVersion, // : ReleaseStep
      commitReleaseVersion, // : ReleaseStep, performs the initial git checks
      tagRelease, // : ReleaseStep
      setNextVersion, // : ReleaseStep
      commitNextVersion, // : ReleaseStep
      pushChanges // : ReleaseStep, also checks that an upstream branch is properly configured
    )
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
      bintrayRepository := "reactivecouchbase-maven",
      pomIncludeRepository := { _ =>
        false
      }
    )
  } else {
    publishCommonsSettings
  }
