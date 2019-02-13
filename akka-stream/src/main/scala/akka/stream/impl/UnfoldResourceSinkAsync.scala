/*
 * Copyright (C) 2015-2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.Done
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts.sameThreadExecutionContext
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.stage._

import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class UnfoldResourceSinkAsync[T, S](
  create:   () ⇒ Future[S],
  readData: (S, T) ⇒ Future[Done],
  close:    (S) ⇒ Future[Done]) extends GraphStage[SinkShape[T]] {
  val in = Inlet[T]("UnfoldResourceSinkAsync.out")
  override val shape = SinkShape(in)
  override def initialAttributes: Attributes = DefaultAttributes.unfoldResourceSinkAsync

  def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) with InHandler {
    lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider
    private implicit def ec = materializer.executionContext
    private var blockingStream: Option[S] = None

    private val createdCallback = getAsyncCallback[Try[S]] {
      case Success(resource) ⇒
        blockingStream = Some(resource)
        pull(in)
      case Failure(t) ⇒ failStage(t)
    }.invokeWithFeedback _

    private val errorHandler: PartialFunction[Throwable, Unit] = {
      case NonFatal(ex) ⇒ decider(ex) match {
        case Supervision.Stop ⇒
          failStage(ex)
        case Supervision.Restart ⇒ restartResource()
        case Supervision.Resume  ⇒ pull(in)
      }
    }

    private val pushCallback = getAsyncCallback[Try[Done]] {
      case Success(_) ⇒ pull(in)
      case Failure(t) ⇒ errorHandler(t)
    }.invoke _

    override def preStart(): Unit = createResource()

    override def onPush(): Unit =
      blockingStream match {
        case Some(resource) ⇒
          try {
            readData(resource, grab(in)).onComplete(pushCallback)(sameThreadExecutionContext)
          } catch errorHandler
        case None ⇒
        // we got a pull but there is no open resource, we are either
        // currently creating/restarting then the pull will be triggered when creating the
        // resource completes, or shutting down and then the push does not matter anyway
      }

    override def postStop(): Unit = {
      blockingStream.foreach(close)
    }

    private def restartResource(): Unit = {
      blockingStream match {
        case Some(resource) ⇒
          // wait for the resource to close before restarting
          close(resource).onComplete(getAsyncCallback[Try[Done]] {
            case Success(Done) ⇒
              createResource()
            case Failure(ex) ⇒ failStage(ex)
          }.invoke)
          blockingStream = None
        case None ⇒
          createResource()
      }
    }

    private def createResource(): Unit = {
      create().onComplete { resource ⇒
        createdCallback(resource).recover {
          case _: StreamDetachedException ⇒
            // stream stopped
            resource match {
              case Success(r)  ⇒ close(r)
              case Failure(ex) ⇒ throw ex // failed to open but stream is stopped already
            }
        }
      }
    }

    setHandler(in, this)

  }
  override def toString = "UnfoldResourceSinkAsync"

}
