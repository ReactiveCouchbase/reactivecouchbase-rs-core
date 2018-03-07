package org.reactivecouchbase.rs.scaladsl

import com.couchbase.client.java.{PersistTo, ReplicateTo}

import scala.concurrent.duration.Duration

sealed trait Settings
case class WriteSettings(expiration: Duration = InfiniteExpiry,
                         persistTo: PersistTo = PersistTo.NONE,
                         replicateTo: ReplicateTo = ReplicateTo.NONE)
