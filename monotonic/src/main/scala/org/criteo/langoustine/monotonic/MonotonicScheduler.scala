package org.criteo.langoustine.monotonic

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import io.circe.{Encoder, Json}
import org.criteo.langoustine.{Scheduler, _}

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.stm._
import scala.concurrent.ExecutionContext.Implicits.global

object MonotonicContext {
  def apply[T: Encoder](execParams: T, execTime: LocalDateTime) = new MonotonicContext[T] {
    def executionParams = execParams

    def executionTime = execTime

    val enc = implicitly[Encoder[T]]

    override def toJson: Json = Json.obj(
      "exec_params" -> enc(execParams),
      "exec_time" -> Json.fromString(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(executionTime))
    )
  }

  implicit def ordering[T] = Ordering.fromLessThan((t1: MonotonicContext[T], t2: MonotonicContext[T]) => t1.executionTime.isBefore(t2.executionTime))
}

trait MonotonicContext[T] extends SchedulingContext {
  /**
    * Parameters of jobs.
    * Each graph execution will provide different values.
    *
    * @return
    */
  def executionParams: T

  /**
    * Graph execution time, used to order executions.
    * This is the time of the completion of the trigger.
    *
    * @return graph execution time
    */
  def executionTime: LocalDateTime

  def enc: Encoder[T]
}

class TriggerJob[T](triggerName: String, trigger: () => Future[T], mon: MonotonicScheduling[T])
  extends Job[MonotonicScheduling[T]](id = triggerName, name = None, description = None, tags = Set.empty, scheduling = mon)(_ => Future.successful(())) {
  def apply() = trigger()
}

case class MonotonicScheduling[T: Encoder](trigger: MonotonicScheduling.Aux[T]) extends Scheduling {
  type Context = MonotonicContext[T]
  type DependencyDescriptor = MonotonicDependency

  val self = this

  val triggerJob = new TriggerJob("event", trigger, self)

  implicit lazy val scheduler: Scheduler[MonotonicScheduling[T]] = new MonotonicScheduler[T](triggerJob)
}

object MonotonicScheduling {
  type Aux[T] = () => Future[T]
}

// TODO: maybe use a partial representation of the context to materialize timeouts, partial executions or filters
// Example: half of the IDs were processed only from job A to job B
trait MonotonicDependency

case object MonotonicDependency extends MonotonicDependency

class MonotonicScheduler[T: Encoder](trigger: TriggerJob[T], maxGraphParallelRuns: Int = 1) extends Scheduler[MonotonicScheduling[T]] {

  type MonotonicJob = Job[MonotonicScheduling[T]]
  // State is defined as a list of successful executions for each job
  // We do not keep track of failed job in the state, they are listed as not executed
  type State = Map[MonotonicContext[T], Set[MonotonicJob]]
  val state = TMap.empty[MonotonicContext[T], Set[MonotonicJob]]

  def addCompletedRuns(finishedExecutions: Set[(MonotonicJob, MonotonicContext[T])])
                      (implicit txn: InTxn) =
    finishedExecutions.foreach { case (job, context) =>
      state.update(context,
        state.get(context).getOrElse(Set.empty) + job)
    }



  def run(graph: Graph[MonotonicScheduling[T]], executor: Executor[MonotonicScheduling[T]], xa: XA) = {
    def runTrigger(): Unit =
      trigger() onSuccess { case res =>
        atomic { implicit txn =>
          state += MonotonicContext[T](res, LocalDateTime.now) -> Set(trigger)
        }
        runTrigger()
      }

    def go(running: Set[(MonotonicJob, MonotonicContext[T], Future[Unit])]): Unit = {
      val (done, stillRunning) = running.partition { case (_, _, futureExec) => futureExec.isCompleted }
      val stateSnapshot = atomic { implicit txn =>
        addCompletedRuns(done.map { case (job, ctx, _) => (job, ctx) })
        state.snapshot
      }

      val toRun = next(
        graph,
        stateSnapshot,
        running
      )
      val newRunning = stillRunning ++ toRun.map { case (job, context) =>
        (job, context, executor.run(job, context))
      }
      Future.firstCompletedOf(utils.Timeout(Duration.create(1, "s")) :: newRunning.map(_._3).toList).
        andThen { case _ => go(newRunning) }
    }
    go(Set.empty)

    def next(graph: Graph[MonotonicScheduling[T]], currentState: State, running: Set[(MonotonicJob, MonotonicContext[T], Future[Unit])])
    : Set[(MonotonicJob, MonotonicContext[T])] = {
      // Get graph runs by execution time
      val runningGraphPerExecTime = running
        .map { case (job, context, future) => (job, context) }
        .groupBy { case (job, context) => context.executionTime }
        .mapValues(_.toMap)

      if (runningGraphPerExecTime.size < maxGraphParallelRuns) {
        // Last executions (sorted by key => batch exec time)
        val lastGraphs: Map[MonotonicContext[T], Set[MonotonicJob]] = currentState.toSeq.sortBy(_._1).takeRight(maxGraphParallelRuns).toMap
        val completeGraph = graph dependsOn trigger


        // For each vertex check if we should run jobLeft
        for {
          (childJob, parentJob, _) <- completeGraph.edges
          (context, lastExecutions) <- lastGraphs
          job <- lastExecutions
          if (job == parentJob) &&
            runningGraphPerExecTime.get(context.executionTime).forall(!_.contains(childJob)) &&
            !currentState(context).contains(childJob)
        } yield childJob -> context
      }
      else
        Set.empty
    }

    runTrigger()
  }
}