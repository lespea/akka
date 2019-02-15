/*
 * Copyright (C) 2015-2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.Done
import akka.annotation.InternalApi
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.stage._

import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class UnfoldResourceSink[T, S](
  create: () ⇒ S,
  write:  (S, T) ⇒ Unit,
  close:  (S) ⇒ Unit) extends GraphStageWithMaterializedValue[SinkShape[T], Future[Done]] {
  val in = Inlet[T]("UnfoldResourceSink.in")
  override val shape = SinkShape(in)
  override def initialAttributes: Attributes = DefaultAttributes.unfoldResourceSource

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val done = Promise[Done]()

    val createLogic = new GraphStageLogic(shape) with InHandler {
      lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider
      var blockingStream = Option.empty[S]
      setHandler(in, this)

      override def preStart(): Unit = {
        blockingStream = Some(create())
        pull(in)
      }

      final override def onPush(): Unit = {
        try {
          blockingStream.foreach(write(_, grab(in)))
        } catch {
          case NonFatal(ex) ⇒
            decider(ex) match {
              case Supervision.Stop ⇒
                blockingStream.foreach(close)
                blockingStream = None
                done.tryFailure(ex)
                failStage(ex)
              case Supervision.Restart ⇒
                restartState()
              case Supervision.Resume ⇒
            }
        }
        pull(in)
      }

      override def onUpstreamFinish(): Unit = closeStage()

      private def restartState(): Unit = {
        blockingStream.foreach(close)
        blockingStream = None
        blockingStream = Some(create())
      }

      private def closeStage(): Unit =
        try {
          blockingStream.foreach(close)
          blockingStream = None
          done.trySuccess(Done)
          completeStage()
        } catch {
          case NonFatal(ex) ⇒
            done.tryFailure(ex)
            failStage(ex)
        }

      override def postStop(): Unit = {
        blockingStream.foreach(close)
      }
    }

    (createLogic, done.future)
  }

  override def toString = "UnfoldResourceSink"
}
