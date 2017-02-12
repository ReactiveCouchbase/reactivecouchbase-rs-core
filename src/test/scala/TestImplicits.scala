import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object TestImplicits {

  implicit class EnhancedObject[T](obj: T) {
    def debug: T = {
      println(if (obj != null) obj.toString else "null")
      obj
    }

    def debug(label: String): T = {
      println(if (obj != null) s"$label: ${obj.toString}" else s"$label: null")
      obj
    }
  }

  implicit class EnhancedFuture[T](fu: Future[T]) {
    def await: T = Await.result(fu, Duration(10, TimeUnit.SECONDS))
  }
}