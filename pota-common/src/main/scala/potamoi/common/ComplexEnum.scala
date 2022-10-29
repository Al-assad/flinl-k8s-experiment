package potamoi.common

import zio.json._

/**
 * Enum based on Scala 2.x Enumeration, automatically deriving ZIO json Codec.
 */
abstract class ComplexEnum extends Enumeration {

  implicit val jsonDecoder: JsonDecoder[Value] = JsonDecoder[String].mapOrFail { name =>
    withNameOps(name) match {
      case Some(value) => Right(value)
      case None        => Left(s"No value found for ${name}")
    }
  }
  implicit val jsonEncoder: JsonEncoder[Value] = JsonEncoder[String].contramap(_.toString)

  def withNameOps(name: String): Option[Value] = values.find(_.toString == name)

}
