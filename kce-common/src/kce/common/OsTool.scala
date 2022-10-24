package kce.common

import zio._

import java.io.File
import scala.reflect.io.Directory

/**
 * Tool for handling OS operation.
 */
object OsTool {

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
    ZIO.attempt(new File(path).toPath.getParent.toFile.mkdirs()) *>
    ZIO.writeFile(path, content)
  }

}
