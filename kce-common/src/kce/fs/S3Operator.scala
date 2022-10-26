package kce.fs

import kce.conf.KceConf
import zio.{IO, ZIO, ZLayer}
import zio.macros.accessible

import java.io.File

/**
 * S3 storage operator
 */
@accessible
trait S3Operator {

  /**
   * Download object from s3 storage.
   */
  def download(s3Path: String, targetPath: String): IO[S3OprErr, File]

  /**
   * Upload file to s3 storage.
   */
  def upload(filePath: String, s3Path: String, contentType: String): IO[S3OprErr, Unit]

  /**
   * Delete s3 object
   */
  def remove(s3Path: String): IO[S3OprErr, Unit]

  /**
   * Tests whether the s3 object exists.
   */
  def exists(s3Path: String): IO[S3OprErr, Boolean]

}

object S3Operator {
  val live = ZLayer(ZIO.service[KceConf].map(new S3OperatorLive(_)))
}
