/*
 * Copyright (C) 2015-2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.annotation.InternalApi
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.stage._

import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class UnfoldResourceSink[T, S](
  create:    () ⇒ S,
  writeData: (S, T) ⇒ Unit,
  close:     (S) ⇒ Unit) extends GraphStage[SinkShape[T]] {
  val in = Inlet[T]("UnfoldResourceSink.out")
  override val shape = SinkShape(in)
  override def initialAttributes: Attributes = DefaultAttributes.unfoldResourceSource

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic with InHandler = new GraphStageLogic(shape) with InHandler {
    lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider
    var open = false
    var blockingStream: S = _
    setHandler(in, this)

    override def preStart(): Unit = {
      blockingStream = create()
      open = true
      pull(in)
    }

    final override def onPush(): Unit = {
      try {
        writeData(blockingStream, grab(in))
      } catch {
        case NonFatal(ex) ⇒
          decider(ex) match {
            case Supervision.Stop ⇒
              open = false
              close(blockingStream)
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
      open = false
      close(blockingStream)
      blockingStream = create()
      open = true
    }

    private def closeStage(): Unit =
      try {
        close(blockingStream)
        open = false
        completeStage()
      } catch {
        case NonFatal(ex) ⇒ failStage(ex)
      }

    override def postStop(): Unit = {
      if (open) close(blockingStream)
    }

  }
  override def toString = "UnfoldResourceSink"
}
