package potamoi.common

import sttp.client3.SttpBackend
import sttp.client3.httpclient.zio.HttpClientZioBackend
import zio.{IO, Task, ZIO}

/**
 * Sttp client tool.
 */
object SttpExtension {

  /**
   * Using Sttp backend to create http request effect and then release the request
   * backend resource automatically.
   */
  def usingSttp[A](request: SttpBackend[Task, Any] => IO[Throwable, A]): IO[Throwable, A] = {
    ZIO.scoped {
      HttpClientZioBackend.scoped().flatMap(backend => request(backend))
    }
  }

  case class ResolveBodyErr(message: String) extends Exception(message)

  /**
   * Collapse the Either left in the RequestT to the ZIO error channel.
   */
  implicit class RequestBodyIOWrapper[A](requestIO: IO[Throwable, Either[String, A]]) {
    def narrowEither: IO[Throwable, A] = requestIO.flatMap {
      case Left(err)  => ZIO.fail(ResolveBodyErr(err))
      case Right(rsp) => ZIO.succeed(rsp)
    }
  }

}
