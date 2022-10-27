package kce.fs

import kce.common.GenericPF
import zio._

import java.io.File
import scala.reflect.io.Directory

/**
 * Local file system operator.
 */
object LfsOperator {

  /**
   * Delete file or directory recursively of given path.
   */
  def rm(path: String): IO[Throwable, Boolean] = ZIO.attemptBlocking {
    new File(path).contra { file =>
      if (file.isDirectory) new Directory(file).deleteRecursively()
      else file.delete()
    }
  }

  /**
   * Write content to file.
   */
  def write(path: String, content: String): IO[Throwable, Unit] = {
    ensureParentDir(path) *> ZIO.writeFile(path, content)
  }

  /**
   * Ensure the parent directory of given path would be created.
   */
  def ensureParentDir(path: String): IO[Throwable, Unit] = ZIO.attempt(new File(path).toPath.getParent.toFile.mkdirs())

  def ensureParentDir(file: File): IO[Throwable, Unit] = ZIO.attempt(file.toPath.getParent.toFile.mkdirs())

}
