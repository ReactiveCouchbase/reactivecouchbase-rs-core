package org.reactivecouchbase.rs.tests

import akka.actor.ActorSystem
import org.reactivecouchbase.rs.scaladsl.CannotRetryException
import org.reactivecouchbase.rs.scaladsl.Retries._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, MustMatchers}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Test suite for retries.
 */
class RetriesTest extends FlatSpec with ScalaFutures with MustMatchers with BeforeAndAfterAll {

  val actorSystem    = ActorSystem()
  implicit val ec    = actorSystem.dispatcher
  implicit val sched = actorSystem.scheduler

  def doSomethingGood(): Future[String] = {
    println("good")
    Future.successful("SUCCESS")
  }

  def doSomethingBad(): Future[String] = {
    println("bad")
    Future.failed(new RuntimeException("FAILURE"))
  }

  "Retries" should "not retry when passing a good computation" in {

    val result = retryOnError(3, 2.millis)(doSomethingGood())

    result.futureValue mustBe ("SUCCESS")

  }

  "Retries" should "fail after max retries when passing a bad computation" in {

    retryOnError(3, 2.millis)(doSomethingBad()).failed.futureValue mustBe an[CannotRetryException]
  }

  override protected def afterAll(): Unit = {
    actorSystem.terminate()
  }

}
