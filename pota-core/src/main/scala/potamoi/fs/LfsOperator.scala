package potamoi.fs

import potamoi.common.{FailStackFill, PotaFail}
import potamoi.syntax.GenericPF
import zio.{IO, ZIO}

import java.io.File

/**
 * Local file system operator.
 */
object LfsOperator {

  /**
   * Tests whether the file denoted by this abstract pathname exists.
   */
  def fileExists(path: String): IO[LfsIOErr, Boolean] = {
    ZIO
      .attempt(new File(path).contra(f => f.exists() && f.isFile))
      .mapError(LfsIOErr)
  }

  /**
   * Delete file or directory recursively of given path.
   */
  def rm(path: String): IO[LfsIOErr, Unit] = ZIO
    .attemptBlocking(os.remove.all(os.Path(new File(path).getAbsolutePath)))
    .mapError(LfsIOErr)

  /**
   * Write content to file.
   */
  def write(path: String, content: String): IO[LfsIOErr, Unit] = {
    ensureParentDir(path) *> ZIO.writeFile(path, content)
  }.mapError {
    case fail: LfsIOErr   => fail
    case cause: Throwable => LfsIOErr(cause)
  }

  /**
   * Ensure the parent directory of given path would be created.
   */
  def ensureParentDir(path: String): IO[LfsIOErr, Unit] = ensureParentDir(new File(path))

  def ensureParentDir(file: File): IO[LfsIOErr, Unit] = {
    ZIO.attempt(file.toPath.getParent.toFile.mkdirs()).unit.mapError(LfsIOErr)
  }

}

case class LfsIOErr(cause: Throwable) extends PotaFail with FailStackFill
