package potamoi.common

import potamoi.common.Color.Color
import potamoi.testkit.STSpec
import zio.json._

class ComplexEnumSpec extends STSpec {

  "ComplexEnum" should {

    "deserialize withName" in {
      Color.withNameOps("Red") shouldBe Some(Color.Red)
      Color.withNameOps("Pink") shouldBe None
    }

    "auto derive zio json codec" in {
      Foo(Color.Red).toJson shouldBe """{"color":"Red"}"""
      """{"color":"Red"}""".fromJson[Foo] shouldBe Right(Foo(Color.Red))
    }

    "tes" in {
      println(Foo(Color.Red).toJson)
    }
  }

}

object Color extends ComplexEnum {
  type Color = Value
  val Red, Yellow, Blue = Value
}

case class Foo(color: Color)
object Foo {
  implicit val codec: JsonCodec[Foo] = DeriveJsonCodec.gen[Foo]
}
