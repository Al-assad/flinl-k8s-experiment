package potamoi.conf

import potamoi.common.ComplexEnum
import potamoi.common.Syntax.GenericPF
import potamoi.conf.S3AccessStyle.{PathStyle, S3AccessStyle, VirtualHostedStyle}
import zio.config.magnolia.name
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * S3 storage configuration.
 */
case class S3Conf(
    @name("endpoint") endpoint: String,
    @name("bucket") bucket: String,
    @name("access-key") accessKey: String,
    @name("secret-key") secretKey: String,
    @name("access-style") accessStyle: S3AccessStyle = S3AccessStyle.PathStyle,
    @name("enable-ssl") sslEnabled: Boolean = false)
    extends Resolvable {

  /**
   * Modify s3 path to the correct access style.
   */
  def revisePath(s3Path: String): String = {
    s3Path.split("://") match {
      case segs if segs.length < 2 => s3Path
      case segs =>
        val revisePathSegs = segs(1).split('/').contra { purePathSegs =>
          accessStyle match {
            case PathStyle          => if (purePathSegs.head == bucket) purePathSegs else Array(bucket) ++ purePathSegs
            case VirtualHostedStyle => if (purePathSegs.head == bucket) purePathSegs.drop(1) else purePathSegs
          }
        }
        segs(0) + "://" + revisePathSegs.mkString("/")
    }
  }

}

object S3Conf {
  implicit val codec: JsonCodec[S3Conf] = DeriveJsonCodec.gen[S3Conf]
}

/**
 * S3 path access style.
 */
object S3AccessStyle extends ComplexEnum {
  type S3AccessStyle = Value
  val PathStyle          = Value("path-style")
  val VirtualHostedStyle = Value("virtual-hosted-style")
}
