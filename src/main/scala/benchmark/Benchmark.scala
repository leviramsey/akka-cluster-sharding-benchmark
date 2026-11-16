package benchmark

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.Done
import akka.actor.Address
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.{ Member, MemberStatus }
import akka.cluster.ClusterEvent.{ MemberEvent, MemberRemoved }
import akka.cluster.typed.{ Cluster, Join, JoinSeedNodes, Subscribe }
import akka.cluster.sharding.typed.ShardingMessageExtractor
import akka.cluster.sharding.typed.scaladsl.{ ClusterSharding, Entity, EntityTypeKey }
import akka.serialization.jackson.CborSerializable
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

object Benchmark {
  private val systemName = "ShardAllocationBenchmark"
  private val versions = 100
  private val membershipTimeout = 2.minutes

  def main(args: Array[String]): Unit = {
    val nodeNumber = args.toList match {
      case raw :: Nil =>
        val parsed = raw.toIntOption.getOrElse(throw new IllegalArgumentException("node number must be an integer in [0, 14]"))
        require(parsed >= 0 && parsed <= 14, "node number must be in [0, 14]")
        parsed
      case _ => throw new IllegalArgumentException("expected exactly one node number in [0, 14]")
    }
    System.setProperty("benchmark.node", nodeNumber.toString)
    LoggerFactory.getLogger("benchmark.Benchmark").info("Starting benchmark node {}", nodeNumber)

    for (appVersion <- 0 until versions) {
      val port = 30000 + nodeNumber + appVersion * 15
      val config = ConfigFactory.parseString(s"""
        akka.actor.provider = cluster
        akka.remote.artery.canonical.hostname = "127.0.0.1"
        akka.remote.artery.canonical.port = $port
        akka.cluster.app-version = "$appVersion"
        akka.cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        akka.cluster.split-brain-resolver.active-strategy = keep-majority
        akka.loglevel = DEBUG
        akka.cluster.sharding.verbose-debug-logging = on
        akka.cluster.sharding.telemetry.instrumentations = ["benchmark.ShardHomeTelemetry"]
        akka.coordinated-shutdown.exit-jvm = off
      """).withFallback(ConfigFactory.load())

      val system = ActorSystem(guardian(nodeNumber, appVersion, port), systemName, config)
      Await.result(system.whenTerminated, Duration.Inf)
      ShardHomeTelemetry.printSummary(nodeNumber)
      Thread.sleep(ThreadLocalRandom.current().nextLong(1500L, 3001L))
    }

    ShardHomeTelemetry.printSummary(nodeNumber)
  }

  private sealed trait GuardianCommand
  private final case class ClusterChanged(event: MemberEvent) extends GuardianCommand
  private case object ShutDown extends GuardianCommand
  private case object Abort extends GuardianCommand

  sealed trait EntityCommand extends CborSerializable {
    def entityId: String
    def replyTo: ActorRef[Done]
  }
  final case class Ping(entityId: String, replyTo: ActorRef[Done]) extends EntityCommand

  private val typeKey = EntityTypeKey[EntityCommand]("BenchmarkEntity")

  private val extractor = new ShardingMessageExtractor[EntityCommand, EntityCommand] {
    override def entityId(message: EntityCommand): String = message.entityId
    override def shardId(entityId: String): String = entityId
    override def unwrapMessage(message: EntityCommand): EntityCommand = message
  }

  private def guardian(nodeNumber: Int, appVersion: Int, port: Int): Behavior[GuardianCommand] =
    Behaviors.setup { context =>
      Behaviors.withTimers[GuardianCommand] { timers =>
        val cluster = Cluster(context.system)
        val selfAddress = cluster.selfMember.uniqueAddress
        val eventAdapter = context.messageAdapter[MemberEvent](ClusterChanged.apply)
        cluster.subscriptions ! Subscribe(eventAdapter, classOf[MemberEvent])

        if (nodeNumber == 0 && appVersion == 0)
          cluster.manager ! Join(cluster.selfMember.address)
        else {
          val seeds = (math.max(port - 29, 30000) to port + 14)
            .map(p => Address("akka", systemName, "127.0.0.1", p))
          cluster.manager ! JoinSeedNodes(seeds)
        }

        timers.startSingleTimer(Abort, membershipTimeout)
        var shardingStarted = false
        var shutdownScheduled = false
        var finalShutdown = false
        var members = Map.empty[akka.cluster.UniqueAddress, Member]

        Behaviors.receiveMessage {
          case ClusterChanged(event) =>
            timers.startSingleTimer(Abort, membershipTimeout)
            event match {
              case removed: MemberRemoved =>
                members -= removed.member.uniqueAddress
                if (removed.member.appVersion.toString == (versions - 1).toString) finalShutdown = true
              case update =>
                members += update.member.uniqueAddress -> update.member
            }

            val up = members.valuesIterator.filter(_.status == MemberStatus.Up).toVector
            if (!shardingStarted && up.exists(_.uniqueAddress == selfAddress) && up.size >= 10) {
              val region = ClusterSharding(context.system).init(
                Entity(typeKey)(_ => Behaviors.receiveMessage[EntityCommand] { command =>
                  command.replyTo ! Done
                  Behaviors.same
                }).withMessageExtractor(extractor))
              val ignoreReply = context.system.ignoreRef[Done]
              (1 to 1000).foreach { id =>
                context.spawn(sender(id.toString, region, ignoreReply), s"sender-$id")
              }
              shardingStarted = true
            }

            if (!shutdownScheduled) {
              val isAmongTwoYoungest = twoYoungest(up).exists(_.uniqueAddress == selfAddress)
              val allSameVersion = up.size == 15 && up.map(_.appVersion).distinct.size == 1
              val shouldShutdown =
                if (finalShutdown) isAmongTwoYoungest
                else if (nodeNumber == 14 && allSameVersion) true
                else if (up.size == 15 && up.map(_.appVersion).distinct.size >= 2) {
                  val lowest = up.map(_.appVersion.toString.toInt).min
                  val oldestVersionMembers = up.filter(_.appVersion.toString.toInt == lowest)
                  twoYoungest(oldestVersionMembers).exists(_.uniqueAddress == selfAddress)
                } else false

              if (shouldShutdown) {
                val delay = if (nodeNumber == 14 && allSameVersion && !finalShutdown) 10.seconds
                else ThreadLocalRandom.current().nextLong(6000L, 8001L).millis
                timers.startSingleTimer(ShutDown, delay)
                shutdownScheduled = true
                context.log.info("Node {} version {} scheduled shutdown in {}", nodeNumber, appVersion, delay)
              }
            }
            Behaviors.same

          case ShutDown =>
            context.system.terminate()
            Behaviors.same

          case Abort =>
            context.log.error("No cluster membership change for {}; aborting node {} version {}", membershipTimeout, nodeNumber, appVersion)
            System.exit(1)
            Behaviors.same
        }
      }
    }

  private def twoYoungest(members: Seq[Member]): Seq[Member] =
    members.sortWith((a, b) => a.isOlderThan(b)).takeRight(2)

  private sealed trait SenderCommand
  private case object Send extends SenderCommand

  private def sender(id: String, region: ActorRef[EntityCommand], ignoreReply: ActorRef[Done]): Behavior[SenderCommand] =
    Behaviors.withTimers { timers =>
      def scheduleNext(): Unit =
        timers.startSingleTimer(Send, ThreadLocalRandom.current().nextLong(1000L, 2501L).millis)
      scheduleNext()
      Behaviors.receiveMessage {
        case Send =>
          region ! Ping(id, ignoreReply)
          scheduleNext()
          Behaviors.same
      }
    }
}
