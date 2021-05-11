package com.github.mnogu.gatling.kafka.action

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.{ActorRef, ActorSystem}
import com.github.mnogu.gatling.kafka.protocol.KafkaProtocol
import com.github.mnogu.gatling.kafka.request.builder.KafkaAttributes
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.Success
import io.gatling.core.CoreComponents
import io.gatling.core.Predef.Session
import io.gatling.core.action.Action
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.controller.throttle.{Throttler, Throttlings}
import io.gatling.core.pause.Disabled
import io.gatling.core.protocol.{ProtocolComponentsRegistry, ProtocolKey}
import io.gatling.core.scenario.SimulationParams
import io.gatling.core.stats.StatsEngine
import io.gatling.core.structure.ScenarioContext
import io.netty.channel.EventLoopGroup
import jodd.util.RandomString
import org.apache.kafka.clients.producer.ProducerConfig
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class KafkaRequestActionBuilderTest extends ScalaTestWithActorTestKit with AnyFlatSpecLike with Matchers with MockFactory with BeforeAndAfterAll {

  behavior of "A kafka request action builder"

  override def afterAll(): Unit = {
    super.afterAll()
  }

  it should "build an action" in {
    val kafkaAttributes: KafkaAttributes[String, Array[Byte]] = KafkaAttributes(getRequestName, Option(getKey), getPayload)
    val scn: ScenarioContext = getScenarioContext
    val next: Action = mock[Action]

    val kafkaRequestActionBuilder: KafkaRequestActionBuilder[String, Array[Byte]] = new KafkaRequestActionBuilder(kafkaAttributes)

    val action = kafkaRequestActionBuilder.build(scn, next)

    action shouldBe an[Action]
  }

  private def getRequestName: Session => Success[String] = {
    val requestNameValue: String = RandomString.get().randomAlpha(10)
    (_: Session) => Success[String](requestNameValue)
  }

  private def getKey: Session => Success[String] = {
    val keyValue: String = RandomString.get().randomAlpha(10)
    (_: Session) => Success[String](keyValue)
  }

  private def getPayload[T]: Session => Success[Array[Byte]] = {
    val payloadValue: Array[Byte] = RandomString.get().randomAlpha(10).getBytes
    (_: Session) => Success[Array[Byte]](payloadValue)
  }

  private def getScenarioContext: ScenarioContext = {
    val protocol: KafkaProtocol = KafkaProtocol("test", Map(ProducerConfig.ACKS_CONFIG -> "1",
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer"))
    val protocolComponentsRegistry: ProtocolComponentsRegistry = new ProtocolComponentsRegistry(getCoreComponents,
      Map(protocol.getClass -> protocol),
      mutable.Map.empty[ProtocolKey[_,_], Any])
    new ScenarioContext(getCoreComponents,
      protocolComponentsRegistry,
      Disabled,
      true)
  }

  private def getCoreComponents: CoreComponents = {
    val name: String = RandomString.get().randomAlpha(10)
    new CoreComponents(testKit.internalSystem.classicSystem,
      mock[EventLoopGroup],
      ActorRef.noSender,
      Throttler.newThrottler(ActorSystem(), new SimulationParams(name,
        List.empty,
        Map.empty,
        Map.empty,
        Disabled,
        Throttlings(None, Map.empty),
        None,
        Seq.empty)),
      mock[StatsEngine],
      mock[Clock],
      mock[Action],
      GatlingConfiguration.loadForTest())
  }
}
