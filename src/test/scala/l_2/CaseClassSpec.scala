package l_2

import org.scalatest.flatspec.AnyFlatSpec

import scala.annotation.tailrec
//import scala.reflect.runtime.universe.{MethodSymbol, typeOf}
import scala.reflect.runtime.universe._

/** Кейс классы наследуют тип Product => .productIterator */
case class Apple(foo: String, bar: Int)

/** testOnly CaseClassSpec */
class CaseClassSpec extends AnyFlatSpec {
  val apple: Apple = Apple("red", 0)
  val ccIter: Iterator[Any] = apple.productIterator

  @tailrec
  final def printIterator[T](iter: Iterator[T]): Unit = {
    if (iter.hasNext) {
      println(iter.next)

      printIterator(iter)
    }

  }

  printIterator(ccIter)
  println

  /** productArity - количество элементов в кейс классе. */
  println(apple.productArity)
  println

  /**
   * Получение имен полей кейс класса
   * https://stackoverflow.com/questions/31189754/get-field-names-list-from-case-class
   */
  println(classOf[Apple].getDeclaredFields.mkString("Array(", ", ", ")"))

  def classAccessors[T: TypeTag]: List[MethodSymbol] =
    typeOf[T]
      .members
      .collect { case m: MethodSymbol if m.isCaseAccessor => m }
      .toList

  println(classAccessors[Apple])

}
