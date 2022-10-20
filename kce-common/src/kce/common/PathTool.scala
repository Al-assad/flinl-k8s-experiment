package kce.common

/**
 * Tools for handling paths.
 */
object PathTool {

  /**
   * remove path schema likes from "s3://bucket/xx.jar" to "bucket/xx.jar".
   */
  def purePath(path: String): String = path.split("://").last.contraPF(p => if (p.startsWith("/")) p.substring(1, p.length) else p)

  /**
   * Remove the stash at the beginning of the path.
   */
  def rmSlashPrefix(path: String): String = if (path.startsWith("/")) path.substring(1, path.length) else path

}
