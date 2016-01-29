package io.scalac.amqp.impl

import io.scalac.amqp.{Connection, Message, Routed}
import monifu.reactive.Observable
import org.reactivestreams.tck.{SubscriberBlackboxVerification, TestEnvironment}
import org.scalatest.testng.TestNGSuiteLike
import org.testng.annotations.AfterSuite
import monifu.concurrent.Implicits.globalScheduler
import scala.concurrent.duration._

class ExchangeSubscriberBlackboxSpecMonifu(defaultTimeout: FiniteDuration) extends SubscriberBlackboxVerification[Routed](
  new TestEnvironment(defaultTimeout.toMillis)) with TestNGSuiteLike {

  def this() = this(300.millis)

  val connection = Connection()

  @AfterSuite def cleanup() = connection.shutdown()

  override def createSubscriber() = connection.publish("nowhere")

  val message = Routed(routingKey = "foo", message = Message())

  def createHelperObservable(elements: Long): Observable[Routed] = elements match {
    /** if `elements` is 0 the `Publisher` should signal `onComplete` immediately. */
    case 0                      ⇒ Observable.empty
    /** if `elements` is [[Long.MaxValue]] the produced stream must be infinite. */
    case Long.MaxValue          ⇒ Observable.fromIterator(Iterator.continually(message))
    /** It must create a `Publisher` for a stream with exactly the given number of elements. */
    case n if n <= Int.MaxValue ⇒ Observable.repeat(message).take(n)
    /** I assume that the number of elements is always less or equal to [[Int.MaxValue]] */
    case n                      ⇒ sys.error("n > Int.MaxValue")
  }

  override def createHelperPublisher(elements: Long) = createHelperObservable(elements).toReactive
  override def createElement(element: Int) = message
}
