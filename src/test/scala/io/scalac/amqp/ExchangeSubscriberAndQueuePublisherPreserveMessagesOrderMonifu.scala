package io.scalac.amqp

import java.util.concurrent.atomic.AtomicInteger

import monifu.concurrent.Implicits.globalScheduler
import monifu.reactive.{Observable, Subscriber}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.util.control.NonFatal

class ExchangeSubscriberAndQueuePublisherPreserveMessagesOrderMonifu extends FlatSpec with Matchers {

  val totalMessages = 100000
  val prefetch = 2
  val expected = new AtomicInteger(1)
  val finishedPromise = Promise[Unit]()
  val finished = finishedPromise.future

  "ExchangeSubscriber and QueuePublisher" should "preserve messages order" in {
    val connection = Connection()
    val consConn = Connection()
    val brokerReady = for {
      e <- connection.exchangeDeclare(Exchange("E2", Direct, durable = false, autoDelete = true))
      q <- connection.queueDeclare(Queue("Q2", autoDelete = true))
      b <- connection.queueBind("Q2", "E2", "q")
    } yield b
    Await.result(brokerReady, 10.seconds)

    //prefetch > 1 causes delivery to stream not ordered
    val qPublisher = consConn.consume(queue = "Q2", prefetch = prefetch)
    val eSubscriber = connection.publish(exchange = "E2", routingKey = "q")

    Observable.fromIterator((1 to totalMessages).iterator)
      .map(i => Message(body = BigInt(i).toByteArray))
      .onSubscribe(Subscriber.fromReactiveSubscriber(eSubscriber))

    Observable.fromReactivePublisher(qPublisher)
      .map(d => BigInt(d.message.body.toArray).toInt)
      .foreach(checkExpected)
    //Test takes 3s on decent PC so I give 10x.
    Await.result(finished, 30.seconds)
    connection.shutdown()
    consConn.shutdown()
  }

  def checkExpected(actual: Int): Unit = {
    val exp = expected.getAndIncrement()
    try {
      actual should equal(exp +- (prefetch - 1))
      if (exp == totalMessages) {
        finishedPromise.success(())
      }
    } catch {
      case NonFatal(ex) =>
        finishedPromise.failure(ex)
    }
  }

}
