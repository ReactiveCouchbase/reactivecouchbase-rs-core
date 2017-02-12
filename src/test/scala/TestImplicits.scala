import java.util.concurrent.TimeUnit

import rx.Observable
import rx.functions.{Action0, Action1}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

object TestImplicits {

  implicit class EnhancedObject(obj: Any) {
    def debug: Any = {
      println(if (obj != null) obj.toString else "null")
      obj
    }

    def debug(label: String): Any = {
      println(if (obj != null) s"$label: ${obj.toString}" else s"$label: null")
      obj
    }
  }

  implicit class EnhancedFuture[T](fu: Future[T]) {
    def await: T = Await.result(fu, Duration(10, TimeUnit.SECONDS))
  }
}