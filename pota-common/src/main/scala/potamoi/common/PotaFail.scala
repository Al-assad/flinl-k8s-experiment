package potamoi.common

import zio.ZIO.succeed
import zio._

import java.io.{PrintWriter, StringWriter}
import scala.annotation.tailrec
import scala.language.implicitConversions
import scala.util.control.NoStackTrace

/**
 * Type-safe Error type.
 */
trait PotaFail {
  override def toString: String      = pprint.apply(this).render
  def toException: PotaFailException = PotaFailException(this)
}

/**
 * Mark a [[PotaFail]] as containing the original Java [[Throwable]] type.
 */
trait FailStackFill {
  def cause: Throwable
}

/**
 * Mark a [[PotaFail]] as a wrapper type for another [[PotaFail]] type.
 */
trait FailProxy {
  def potaFail: PotaFail
}

/**
 * Provides compatibility with the Throwable API.
 */
final case class PotaFailException(fail: PotaFail) extends NoStackTrace

final case class FutureException[T](reason: T) extends NoStackTrace

object PotaFail extends PotaFailExtension

trait PotaFailExtension {

  /**
   * Dump the stack trace of Throwable to String.
   */
  def dumpStackTrace(e: Throwable): String = {
    val sw = new StringWriter
    e.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  implicit class PotaFailZIOWrapper[R, E, A](zio: ZIO[R, E, A]) {

    /**
     * Similar to [[ZIO.debug]], but with [[FailStackFill]] appended to the error output content.
     */
    @inline def debugStack(implicit trace: Trace): ZIO[R, E, A] =
      zio.debug
        .tapError {
          case e: PotaFail => debugPotaFail(e)
          case _           => ZIO.unit
        }

    /**
     * See [[debugStack]].
     */
    @inline def debugStack(prefix: => String)(implicit trace: Trace): ZIO[R, E, A] =
      zio
        .debug(prefix)
        .tapError {
          case e: PotaFail => debugPotaFail(e)
          case _           => ZIO.unit
        }

    @tailrec
    private def debugPotaFail(fail: PotaFail): UIO[Unit] =
      fail match {
        case e: FailProxy     => debugPotaFail(e.potaFail)
        case e: FailStackFill => ZIO.succeed(e.cause.printStackTrace())
        case _                => ZIO.unit
      }
  }

  /**
   * Recursively find the original Java stack trace marked by [[FailStackFill]].
   */
  def findJavaFailStack(fail: PotaFail): UIO[Option[String]] = {
    succeed(fail).flatMap {
      case e: FailProxy     => findJavaFailStack(e.potaFail)
      case e: FailStackFill => ZIO.succeed(Some(dumpStackTrace(e.cause)))
      case _                => ZIO.succeed(None)
    }
  }

}
