package io.scalac.amqp

import java.util.concurrent.{ForkJoinPool, ThreadPoolExecutor}

import com.typesafe.config.{ConfigFactory, Config}
import io.scalac.amqp.impl.ConfigToExecutorService
import org.scalatest.{Inside, Matchers, FlatSpec}

class ConfigToExecutorServiceSpec  extends FlatSpec with Matchers with Inside {

  "default" should "be mapped to None" in {
    val cfg = """amqp {
                   executor = "default"
                 }""" 
    ConfigToExecutorService(ConfigFactory.parseString(cfg)) shouldBe None
  }
  
  "thread-pool-executor" should "regard 'threads-min'" in {
    val min = Runtime.getRuntime.availableProcessors() * 2
    val cfg =
      s"""amqp {
            executor = "thread-pool-executor"
            thread-pool-executor {
              threads-min = $min
              threads-factor = 1
              threads-max = ${min * 2}
            }
          }
      """
    val result = ConfigToExecutorService(ConfigFactory.parseString(cfg))
    inside (result) {
      case Some(executorService) =>
        executorService shouldBe a [ThreadPoolExecutor]
        executorService.asInstanceOf[ThreadPoolExecutor].getCorePoolSize shouldBe min
    }
  }

  "work-stealing-pool" should "regard 'parallelism-max'" in {
    val max = Runtime.getRuntime.availableProcessors() * 2
    val cfg =
      s"""amqp {
            executor = "work-stealing-pool"
            work-stealing-pool {
              parallelism-min = 1
              parallelism-factor = 3
              parallelism-max = $max
            }
          }
      """
    val result = ConfigToExecutorService(ConfigFactory.parseString(cfg))
    inside (result) {
      case Some(executorService) =>
        executorService shouldBe a [ForkJoinPool]
        executorService.asInstanceOf[ForkJoinPool].getParallelism shouldBe max
    }
  }
}
