package org.reactivecouchbase.rs.scaladsl

import com.couchbase.client.java.{PersistTo, ReplicateTo}

import scala.concurrent.duration.Duration

sealed trait Settings
case class WriteSettings(expiration: Duration = Duration.Zero, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)
