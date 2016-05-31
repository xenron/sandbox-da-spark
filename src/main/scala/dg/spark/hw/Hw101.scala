package dg.spark.hw

object Hw101 {
  def main(args: Array[String]) {
    val clock1 = new MyClock(10, 11)
    println(clock1.toString())
    val clock2 = new MyClock(10, 11, 12)
    println(clock2.toString())
    clock2.doSomething(clock2)
    println(clock2.toString())

    val watch1 = new MyWatch(10, 11)
    watch1.alert(10, 10)
    watch1.alert(10, 11)
  }
}

abstract class AbsTime {
  var hours: Int

  def getHours() = {
    this.hours
  }

  var minutes: Int

  def getMinutes() = {
    this.minutes
  }

  var second: Int

  def getSecond() = {
    this.second
  }

}

trait AddTime {
  var hours: Int = 0;

  var minutes: Int = 0;

  var second: Int = 0;

  def doSomething(c: MyClock) = {
    var hours: Int = c.hours
    var minutes: Int = c.minutes
    var second: Int = c.second
    if (minutes == 0 && second == 0) println("Hours is " + hours)
    if (second == 30) {
      second = 0;
      if (minutes == 59) {
        minutes = 0
        if (hours < 23) {
          hours = hours + 1
        } else {
          hours = 0
        }
      } else {
        minutes = minutes + 1
      }
    } else {
      second = 30
    }
    if (minutes == 0 && second == 0) println("Hours is " + hours)
    c.hours = hours;
    c.minutes = minutes;
    c.second = second;
  }
}

class MyClock extends AbsTime with AddTime {


  def this(h: Int, min: Int) {
    this()
    this.hours = h
    this.minutes = min
  }

  def this(h: Int, min: Int, sec: Int) {
    this(h, min)
    this.second = sec
  }

  override def toString(): String = {
    hours + " : " + minutes + " : " + second
  }
}

class MyWatch extends AbsTime with AddTime {

  def this(h: Int, min: Int) {
    this()
    this.hours = h
    this.minutes = min
  }

  def alert(hour: Int, min: Int) {
    //  if(hour==this.hours&&min==this.minutes)println("Alert!! Time is "+h+ " : "+min)
    if (hour == this.hours && min == this.minutes) println("Alert!! Time is " + hour + " : " + min)
  }

}