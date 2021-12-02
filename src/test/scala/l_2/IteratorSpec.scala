package l_2

import org.scalatest.flatspec.AnyFlatSpec

import scala.annotation.tailrec

/** testOnly l_2.IteratorSpec */
class IteratorSpec extends AnyFlatSpec {
  val list: List[Int] = List(-1, 2, 3)
  val condition: Int => Boolean = x => x > 0
  val partition: Iterator[Int] = list.iterator  // в реальной жизни - Iterator[InternalRow]

  val filteredIter: Iterator[Int] = partition.filter(condition)

  @tailrec
  final def printIterator[T](iter: Iterator[T]): Unit = {
    if (iter.hasNext) {
      println(iter.next)
      printIterator(iter)
    }
  }

  printIterator(filteredIter)
}
