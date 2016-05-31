package dg.spark.hw

object Hw102 {
  def main(args: Array[String]) {
    println("----- original array -----")
    val array = Array[Int](0, 1, 2, 3, 4, 5, 6, 7, 8)
    println(array.mkString(" "))

    println("----- first modify -----")
    val temp1 = changeLocation[Int](2, 3) _
    temp1(array)

    println("----- second modify -----")
    val temp2 = changeLocation[Int](1, 7) _
    temp2(array)

    println("----- third modify -----")
    val temp3 = changeLocation[Int](5, 10) _
    temp3(array)
  }

  def changeLocation[T](x: Int, y: Int)(array: Array[T]): Unit = {
    (x, y) match {

      case _ if x > array.length => {
        println("x out of Index")
      }
      case _ if y > array.length => {
        println("y out of Index")
      }
      case _ if x == y => {
        println("x can't eque y")
      }
      case _ => {

        println("befare change : ")
        println(array.mkString(", "))
        val temp = array(x - 1)
        array(x - 1) = array(y - 1)
        array(y - 1) = temp
        println("after change : ")
        println(array.mkString(", "))
      }
    }
  }
}
