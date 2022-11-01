package potamoi.fs

import io.minio._
import io.minio.errors.ErrorResponseException
import potamoi.common.GenericPF
import potamoi.common.PathTool.purePath
import potamoi.conf.PotaConf
import potamoi.conf.S3AccessStyle.PathStyle
import potamoi.fs.S3Err._
import zio.ZIO.succeed
import zio._
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
  def download(s3Path: String, targetPath: String): IO[S3Err, File]

  /**
   * Upload file to s3 storage.
   */
  def upload(filePath: String, s3Path: String, contentType: String): IO[S3Err, Unit]

  /**
   * Delete s3 object
   */
  def remove(s3Path: String): IO[S3Err, Unit]

  /**
   * Tests whether the s3 object exists.
   */
  def exists(s3Path: String): IO[S3Err, Boolean]

}

object S3Operator {

  val live = ZLayer(ZIO.service[PotaConf].map(new Live(_)))

  class Live(kceConf: PotaConf) extends S3Operator {
    private val s3Conf = kceConf.s3

    private val minioClient = MinioClient
      .builder()
      .endpoint(s3Conf.endpoint)
      .credentials(s3Conf.accessKey, s3Conf.secretKey)
      .build()

    /**
     * Download object from s3 storage.
     */
    override def download(s3Path: String, targetPath: String): IO[S3Err, File] = {
      for {
        objectName <- extractObjectName(s3Path)
        _ <- lfs
          .ensureParentDir(targetPath)
          .mapError(e => IOErr(s"Fail to create parent directory of target file: $targetPath.", e.cause))
        _ <- ZIO
          .attemptBlockingInterrupt {
            minioClient.downloadObject(
              DownloadObjectArgs
                .builder()
                .bucket(s3Conf.bucket)
                .`object`(objectName)
                .filename(targetPath)
                .overwrite(true)
                .build()
            )
            new File(targetPath)
          }
          .mapError(DownloadObjErr(s3Path, objectName, S3Err.S3Location(s3Conf), _))
        file <- ZIO.succeed(new File(targetPath))
      } yield file
    }

    /**
     * Upload file to s3 storage.
     */
    override def upload(filePath: String, s3Path: String, contentType: String): IO[S3Err, Unit] = {
      for {
        _ <- ZIO
          .succeed(new File(filePath).contra(f => f.exists() && f.isFile))
          .flatMap(ZIO.fail(FileNotFound(filePath)).unless(_))
        objectName <- extractObjectName(s3Path)
        _ <- ZIO
          .attemptBlockingInterrupt {
            minioClient.uploadObject(
              UploadObjectArgs
                .builder()
                .bucket(s3Conf.bucket)
                .`object`(objectName)
                .filename(filePath)
                .contentType(contentType)
                .build())
          }
          .mapError(UploadObjErr(s3Path, objectName, S3Err.S3Location(s3Conf), _))
      } yield ()
    }

    /**
     * Delete s3 object
     */
    override def remove(s3Path: String): IO[S3Err, Unit] = {
      extractObjectName(s3Path).flatMap { objectName =>
        ZIO
          .attemptBlockingInterrupt {
            minioClient.removeObject(RemoveObjectArgs.builder().bucket(s3Conf.bucket).`object`(objectName).build())
          }
          .mapError(RemoveObjErr(s3Path, objectName, S3Err.S3Location(s3Conf), _))
      }
    }

    /**
     * Tests whether the s3 object exists.
     */
    override def exists(s3Path: String): IO[S3Err, Boolean] = {
      extractObjectName(s3Path).flatMap { objectName =>
        ZIO
          .attemptBlockingInterrupt {
            minioClient.getObject(GetObjectArgs.builder().bucket(s3Conf.bucket).`object`(objectName).build())
          }
          .as(true)
          .catchSome { case e: ErrorResponseException if e.errorResponse().code() == "NoSuchKey" => succeed(false) }
          .mapError(GetObjErr(s3Path, objectName, S3Err.S3Location(s3Conf), _))
      }
    }

    private def extractObjectName(s3Path: String): UIO[String] = ZIO.succeed {
      val path = purePath(s3Path)
      val segs = path.split('/')
      if (segs(0) == s3Conf.bucket && s3Conf.accessStyle == PathStyle) segs.drop(1).mkString("/")
      else path
    }
  }

}
