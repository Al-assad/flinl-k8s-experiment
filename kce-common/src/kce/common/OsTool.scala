package kce.common

import zio._

import java.io.{File, IOException}
import scala.reflect.io.Directory

/**
 * Tool for handling OS operation.
 */
object OsTool {

  /**
   * Delete file or directory recursively of given path.
   */
  def rm(path: String): IO[Throwable, Boolean] = ZIO.attemptBlocking {
    new File(path).contraPF { file =>
      if (file.isDirectory) new Directory(file).deleteRecursively()
      else file.delete()
    }
  }

  /**
   * Write content to file.
   */
  def write(path: String, content: String): IO[IOException, Unit] = ZIO.writeFile(path, content)

}
