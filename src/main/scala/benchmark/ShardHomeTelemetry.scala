package benchmark

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder

import akka.actor.ExtendedActorSystem
import akka.cluster.sharding.internal.ClusterShardingInstrumentation
import org.HdrHistogram.ConcurrentHistogram

/** One instance is constructed for each actor system; measurements survive those systems. */
final class ShardHomeTelemetry extends ClusterShardingInstrumentation {
  def this(system: ExtendedActorSystem) = this()

  private val requestedAt = new ConcurrentHashMap[String, java.lang.Long]()

  override def dependencies: Seq[String] = Seq.empty

  override def regionRequestedShardHome(typeName: String, shardId: String): Unit = {
    requestedAt.putIfAbsent(shardId, System.nanoTime())
    ()
  }

  override def receivedShardHome(typeName: String, shardId: String): Unit = {
    val started = requestedAt.remove(shardId)
    if (started != null) {
      val elapsed = System.nanoTime() - started.longValue()
      ShardHomeTelemetry.record(elapsed)
    }
  }

  override def shardRegionBufferSize(typeName: String, size: Int): Unit = ()
  override def shardRegionBufferSizeIncremented(typeName: String): Unit = ()
  override def messageDropped(typeName: String): Unit = ()
  override def shardHandoffStarted(typeName: String, shard: String): Unit = ()
  override def shardHandoffFinished(typeName: String, shard: String, ok: Boolean): Unit = ()
}

object ShardHomeTelemetry {
  private val maxNanos = TimeUnit.SECONDS.toNanos(10)
  private val histogram = new ConcurrentHistogram(1000L, maxNanos, 3)
  private val overflow = new LongAdder

  private[benchmark] def record(nanos: Long): Unit = {
    if (nanos > maxNanos) overflow.increment()
    histogram.recordValue(math.max(1000L, math.min(nanos, maxNanos)))
  }

  def printSummary(nodeNumber: Int): Unit = {
    val snapshot = histogram.copy()
    def millis(percentile: Double): Double =
      snapshot.getValueAtPercentile(percentile).toDouble / 1000000.0

    println(
      f"node=$nodeNumber samples=${snapshot.getTotalCount}%d " +
        f"p50=${millis(50)}%.3f ms p90=${millis(90)}%.3f ms " +
        f"p99=${millis(99)}%.3f ms p99.9=${millis(99.9)}%.3f ms " +
        s"over_10s=${overflow.sum()}")
  }
}
