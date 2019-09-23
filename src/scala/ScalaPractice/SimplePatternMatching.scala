package ScalaPractice

object SimplePatternMatching {

  def main(args: Array[String]): Unit = {
/*    println(matchingTest(2))
    println(matchingTest(3))*/

    println(matchingTestAny("one"))
    println(matchingTestAny(2))
    println(matchingTestAny(true))
    println(matchingTestAny(false))
  }

  def matchingTest(x: Int) :String = x match {
    case 1 => "one"
    case 2 => "two"
    case _ => throw new IndexOutOfBoundsException("value not found ")
  }

  def matchingTestAny(x: Any) :Any = x match {
    case "one" => 1
    case 2 => "two"
    case true  => "value is true"
    case _ => throw  new IndexOutOfBoundsException("value not found")
  }
}
