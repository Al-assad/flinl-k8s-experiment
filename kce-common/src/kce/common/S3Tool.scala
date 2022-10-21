package kce.common

object S3Tool {

  /**
   * S3 storage prefix
   */
  val s3SchemaPrefix = Vector("s3", "s3a", "s3n", "s3p")

  /**
   * Determine if the file path is s3 schema.
   */
  def isS3Path(path: String): Boolean = s3SchemaPrefix.contains(path.split("://").head.trim)

}
