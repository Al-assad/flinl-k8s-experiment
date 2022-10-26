package kce.fs

import kce.common.SilentErr

/**
 * S3 operation error
 */
sealed class S3OprErr(msg: String, cause: Throwable) extends Exception(msg: String, cause: Throwable)

case class DownloadS3ObjErr(msg: String, cause: Throwable) extends S3OprErr(msg, cause)

case class UploadS3ObjErr(msg: String, cause: Throwable = SilentErr) extends S3OprErr(msg, cause)

case class RemoveS3ObjErr(msg: String, cause: Throwable = SilentErr) extends S3OprErr(msg, cause)

case class GetObjectErr(msg: String, cause: Throwable = SilentErr) extends S3OprErr(msg, cause)
