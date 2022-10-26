package kce.fs

import io.minio.errors.ErrorResponseException
import io.minio.{DownloadObjectArgs, GetObjectArgs, MinioClient, RemoveObjectArgs, UploadObjectArgs}
import kce.common.GenericPF
import kce.common.LogMessageTool.LogMessageStringWrapper
import kce.common.PathTool.purePath
import kce.conf.KceConf
import kce.conf.S3AccessStyle.PathStyle
import zio.ZIO.{fail, succeed}
import zio.{IO, UIO, ZIO}

import java.io.File

class S3OperatorLive(kceConf: KceConf) extends S3Operator {

  private val s3Conf = kceConf.s3

  private val minioClient = MinioClient
    .builder()
    .endpoint(s3Conf.endpoint)
    .credentials(s3Conf.accessKey, s3Conf.secretKey)
    .build()

  /**
   * Download object from s3 storage.
   */
  override def download(s3Path: String, targetPath: String): IO[S3OprErr, File] = {
    for {
      objectName <- extractObjectName(s3Path)
      _ <- lfs
        .ensureParentDir(s3Path)
        .mapError(DownloadS3ObjErr("Fail to create parent directory of target file." tag "path" -> targetPath, _))
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
        .mapError(
          DownloadS3ObjErr(
            "Fail to download s3 object." tag Map(
              "s3.Path"       -> s3Path,
              "s3.objectName" -> objectName,
              "s3.bucket"     -> s3Conf.bucket,
              "s3.endpoint"   -> s3Conf.endpoint),
            _))
      file <- ZIO.succeed(new File(targetPath))
    } yield file
  }

  /**
   * Upload file to s3 storage.
   */
  override def upload(filePath: String, s3Path: String, contentType: String): IO[S3OprErr, Unit] = {
    for {
      _          <- fail(UploadS3ObjErr("File not exists." tag "path" -> filePath)).unless(new File(filePath).contra(f => f.exists() && f.isFile))
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
              .build()
          )
        }
        .mapError(
          UploadS3ObjErr(
            "Fail to upload file." tag Map(
              "path"        -> filePath,
              "s3.Path"     -> s3Path,
              "s3.object"   -> objectName,
              "s3.bucket"   -> s3Conf.bucket,
              "s3.endpoint" -> s3Conf.endpoint),
            _))
    } yield ()
  }

  /**
   * Delete s3 object
   */
  override def remove(s3Path: String): IO[S3OprErr, Unit] = {
    for {
      objectName <- extractObjectName(s3Path)
      _ <- ZIO
        .attemptBlockingInterrupt {
          minioClient.removeObject(
            RemoveObjectArgs
              .builder()
              .bucket(s3Conf.bucket)
              .`object`(objectName)
              .build()
          )
        }
        .mapError(
          RemoveS3ObjErr(
            "Fail to remove s3 object" tag Map(
              "s3.path"     -> s3Path,
              "s3.object"   -> objectName,
              "s3.bucket"   -> s3Conf.bucket,
              "s3.endpoint" -> s3Conf.endpoint),
            _))
    } yield ()
  }

  /**
   * Tests whether the s3 object exists.
   */
  override def exists(s3Path: String): IO[S3OprErr, Boolean] = {
    for {
      objectName <- extractObjectName(s3Path)
      rsp <- ZIO
        .attemptBlockingInterrupt {
          minioClient.getObject(
            GetObjectArgs
              .builder()
              .bucket(s3Conf.bucket)
              .`object`(objectName)
              .build())
        }
        .as(true)
        .catchSome { case e: ErrorResponseException if e.errorResponse().code() == "NoSuchKey" => succeed(false) }
        .mapError(
          GetObjectErr(
            "Fail to get s3 object" tag Map(
              "s3.path"     -> s3Path,
              "s3.object"   -> objectName,
              "s3.bucket"   -> s3Conf.bucket,
              "s3.endpoint" -> s3Conf.endpoint),
            _)
        )
    } yield rsp
  }

  private def extractObjectName(s3Path: String): UIO[String] = ZIO.succeed {
    purePath(s3Path).contra { path =>
      path.split('/').contra { segs =>
        if (segs(0) == s3Conf.bucket && s3Conf.accessStyle == PathStyle) segs.drop(1).mkString("/") else path
      }
    }
  }

}
