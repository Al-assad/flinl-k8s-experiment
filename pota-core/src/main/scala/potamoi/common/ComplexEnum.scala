package potamoi.common

import io.getquill.MappedEncoding
import zio.config.magnolia.Descriptor
import zio.json._

/**
 * Enum based on Scala 2.x Enumeration, automatically deriving ZIO json Codec.
 */
abstract class ComplexEnum extends Enumeration {

  // ZIO-Json encoder / decoder
  implicit val jsonEncoder: JsonEncoder[Value] = JsonEncoder[String].contramap(_.toString)
  implicit val jsonDecoder: JsonDecoder[Value] = JsonDecoder[String].mapOrFail { name =>
    withNameOps(name) match {
      case Some(value) => Right(value)
      case None        => Left(s"No value found for ${name}")
    }
  }

  // ZIO-config descriptor
  implicit val configDesc: Descriptor[Value] = Descriptor[String].transform(string => withName(string), _.toString)

  // quill encoder / decoder
  implicit val quillEncoder = MappedEncoding[Value, String](_.toString)
  implicit val quillDecoder = MappedEncoding[String, Value](withName)

  def withNameOps(name: String): Option[Value] = values.find(_.toString == name)

}
