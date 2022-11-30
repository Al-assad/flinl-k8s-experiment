package potamoi

import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.client.{CodingFailure, DeserializationFailure, K8sFailure, RequestFailure}
import io.circe.Errors
import zio.prelude.data.Optional
import zio.prelude.data.Optional.{Absent, Present}

import scala.language.implicitConversions

package object k8s {

  /**
   * Find direct Java Throwable from [[K8sFailure]].
   */
  def liftException(k8sOperator: K8sFailure): Option[Throwable] = {
    k8sOperator match {
      case CodingFailure(_, failure)         => Some(failure)
      case RequestFailure(_, reason)         => Some(reason)
      case DeserializationFailure(_, errors) => Some(Errors(errors))
      case _                                 => None
    }
  }

  /**
   * Convert [[String]] to [[K8sNamespace]].
   */
  implicit def stringToK8sNamespace(namespace: String): K8sNamespace = K8sNamespace(namespace)

  /**
   * Convert [[Option]] to [[Optional]].
   */
  implicit class OptionOps[A](option: Option[A]) {
    def toOptional: Optional[A] = option match {
      case None    => Absent
      case Some(v) => Present(v)
    }
  }
}
