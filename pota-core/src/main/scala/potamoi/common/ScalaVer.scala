package potamoi.common

/**
 * Scala major version.
 */
object ScalaVer extends ComplexEnum {
  type ScalaVer = Value

  val Scala211 = Value("2.11")
  val Scala212 = Value("2.12")
  val Scala213 = Value("2.13")
  val Scala3   = Value("3")
}
