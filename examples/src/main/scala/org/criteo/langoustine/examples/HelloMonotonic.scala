package org.criteo.langoustine.examples

import org.criteo.langoustine.monotonic._
import org.criteo.langoustine.{Job, Langoustine, utils}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

object HelloMonotonic {

  def main(args: Array[String]) = {

    val monotonicScheduling: MonotonicScheduling[Int] = MonotonicScheduling(() => {
      utils.Timeout(Duration.create(1, "min")) map { _ => Math.random().toInt }
    })

    val world = Job("world", scheduling = monotonicScheduling) {
      exec => Future(println(s"WORLD!!! ${exec.context.executionParams} || ${exec.context.executionTime}"))
    }

    val hello = Job("hello", scheduling = monotonicScheduling) {
      _ => Future(println("HELLO!!!!!"))
    }

    import monotonicScheduling.scheduler

    Langoustine("Hello World") {
      world dependsOn hello
    }.run()
  }

}
