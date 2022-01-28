package l_4

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class FlatOptionSpec extends AnyFlatSpec with should.Matchers {
  def getPositive(i: Int): Option[Int] =
    if (i > 0) Some(i) else None


  "Options" should "work" in {
    val maybeNumbers: List[Option[Int]] = List(Some(1), Some(2), None, Some(3))
    maybeNumbers.flatten shouldBe List(1, 2, 3)
  }

  it should "work even better" in {
    val numbers: List[Int] = List(-3, -2, -1, 0, 1, 2, 3)
    numbers.map(getPositive) shouldBe List(None, None, None, None, Some(1), Some(2), Some(3))
    numbers.flatMap(getPositive) shouldBe List(1, 2, 3)
  }

}
