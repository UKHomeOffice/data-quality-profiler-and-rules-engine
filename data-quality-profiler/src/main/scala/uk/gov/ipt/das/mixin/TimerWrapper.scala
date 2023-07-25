package uk.gov.ipt.das.mixin

trait TimerWrapper {

  def timer(name: String, iterations: Int = 1)(callback: => Unit): Unit = {
    val timer = TimerWrapperTimer(name)
    for (_ <- 1 to iterations) callback
    timer.stop(iterations = iterations)
  }

}
sealed class TimerWrapperTimer private (val name: String) {

  private val startTime = System.currentTimeMillis()

  println(s"Starting timer [$name] at $startTime")

  def stop(iterations: Int): Unit = {
    val endTime = System.currentTimeMillis()
    println(
      s"""Stopping timer [$name] at $endTime,
         | $iterations iteration${if (iterations != 1) "s"},
         | runtime in milliseconds: ${endTime - startTime},
         | in seconds: ${(endTime - startTime) / 1000},
         | in minutes: ${((endTime - startTime) / 1000) / 60}""".stripMargin)
  }

}
object TimerWrapperTimer {
  def apply(name: String): TimerWrapperTimer = new TimerWrapperTimer(name = name)
}