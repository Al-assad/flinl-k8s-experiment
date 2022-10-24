package kce.common

/**
 * Type-safe Exception implementation.
 */
case class Err(msg: String, cause: Throwable = SilentErr) extends Exception(msg, cause) {}

case object SilentErr extends Exception

/**
 * Tool for logging message.
 */
object LogMessageTool {

  /**
   * Append golang-style tags to message content.
   */
  @inline def amendTags(message: String, tags: Map[String, String]): String =
    message + " " + tags.map { case (k, v) => s"[$k=$v]" }.mkString(" ")

  implicit class LogMessageStringWrapper(msg: String) {
    @inline def <>(tags: Map[String, String]): String = amendTags(msg, tags)
    @inline def <>(tags: (String, String)*): String   = amendTags(msg, tags.toMap)
  }
}
