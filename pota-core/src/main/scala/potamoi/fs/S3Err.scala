package potamoi.fs

import potamoi.common.{FailStackFill, PotaFail}
import potamoi.config.S3AccessStyle.S3AccessStyle
import potamoi.config.S3Conf

/**
 * S3 operation failure.
 */
sealed trait S3Err extends PotaFail

object S3Err {
  case class IOErr(msg: String, cause: Throwable) extends S3Err with FailStackFill
  case class FileNotFound(path: String)           extends S3Err

  case class DownloadObjErr(s3Path: String, objName: String, point: S3Location, cause: Throwable) extends S3Err with FailStackFill
  case class UploadObjErr(s3Path: String, objName: String, point: S3Location, cause: Throwable)   extends S3Err with FailStackFill
  case class RemoveObjErr(s3Path: String, objName: String, point: S3Location, cause: Throwable)   extends S3Err with FailStackFill
  case class GetObjErr(s3Path: String, objName: String, point: S3Location, cause: Throwable)      extends S3Err with FailStackFill

  case class S3Location(bucket: String, endpoint: String, accStyle: S3AccessStyle)
  object S3Location {
    def apply(s3Conf: S3Conf): S3Location = S3Location(s3Conf.bucket, s3Conf.endpoint, s3Conf.accessStyle)
  }
}
