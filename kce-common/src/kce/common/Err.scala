package kce.common

/**
 * Type-safe Exception implementation.
 */
case class Err(msg: String, cause: Throwable = SilentErr) extends Exception(msg, cause)

case object SilentErr extends Exception

/**
 * Tool for logging message.
 */
object LogMessageTool {

  /**
   * Append golang-style tags to message content.
   */
  @inline def prettyLogTags(tags: Map[String, String]): String = tags.map { case (k, v) => s"[$k=$v]" }.mkString(" ")

  implicit class LogMessageStringWrapper(msg: String) {
    @inline def tag(tags: Map[String, String]): String = s"$msg ${prettyLogTags(tags)}"
    @inline def tag(tags: (String, String)*): String   = tag(tags.toMap)
  }
}
