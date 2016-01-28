package io.scalac.amqp.impl

import java.util.concurrent.{ForkJoinPool, Executors, ExecutorService}

import com.typesafe.config.Config

object ConfigToExecutorService {

  def apply(config: Config): Option[ExecutorService] = {
    config.getString("amqp.executor") match {
      case "default" =>
        None
      case "thread-pool-executor" =>
        Some(threadPoolExecutor(config.getConfig("amqp.thread-pool-executor")))
      case "work-stealing-pool" =>
        Some(workStealingPool(config.getConfig("amqp.work-stealing-pool")))
    }
  }

  def threadPoolExecutor(config: Config): ExecutorService =
    Executors.newFixedThreadPool(
      boundedAvailableProcessors(
        config.getInt("threads-min"),
        config.getInt("threads-factor"),
        config.getInt("threads-max")
      ))


  def workStealingPool(config: Config): ExecutorService = {
    val parallelism =
      boundedAvailableProcessors(
        config.getInt("parallelism-min"),
        config.getInt("parallelism-factor"),
        config.getInt("parallelism-max")
      )
    new ForkJoinPool(parallelism, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true)
  }

  def boundedAvailableProcessors(min: Int, factor: Int, max: Int) =
    Math.min(Math.max(factor * Runtime.getRuntime.availableProcessors(), min), max)
}
