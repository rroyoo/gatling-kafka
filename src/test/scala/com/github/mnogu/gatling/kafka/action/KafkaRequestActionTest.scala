package com.github.mnogu.gatling.kafka.action

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.{ActorRef, ActorSystem}
import com.github.mnogu.gatling.kafka.protocol.KafkaProtocol
import com.github.mnogu.gatling.kafka.request.builder.KafkaAttributes
import io.gatling.commons.util.{Clock, DefaultClock}
import io.gatling.commons.validation.Success
import io.gatling.core.CoreComponents
import io.gatling.core.action.{Action, Exit}
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.controller.throttle.{Throttler, Throttlings}
import io.gatling.core.pause.Disabled
import io.gatling.core.scenario.SimulationParams
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.netty.channel.{DefaultEventLoop, EventLoopGroup}
import jodd.util.RandomString
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class KafkaRequestActionTest extends ScalaTestWithActorTestKit with AnyFlatSpecLike with Matchers with MockFactory with BeforeAndAfterAll {

  behavior of "A kafka request action"

  override def afterAll(): Unit = {
    super.afterAll()
  }

  it should "send an avro event to kafka" in {
    val kafkaProducer: KafkaProducer[String, GenericRecord] = getKafkaProducer
    val kafkaAttributes: KafkaAttributes[String, GenericRecord] = KafkaAttributes(getRequestName, Option(getKey), getPayload)
    new KafkaRequestAction(kafkaProducer,
      kafkaAttributes,
      getCoreComponents,
      getKafkaProtocol,
      false,
      new Exit(ActorRef.noSender, new DefaultClock)
    ).execute(Session("test2", 1L, new DefaultEventLoop()))
  }

  private def getRequestName: Session => Success[String] = {
    val requestNameValue: String = RandomString.get().randomAlpha(10)
    (_: Session) => Success[String](requestNameValue)
  }

  private def getKey: Session => Success[String] = {
    val keyValue: String = RandomString.get().randomAlpha(10)
    (_: Session) => Success[String](keyValue)
  }

  private def getPayload[T]: Session => Success[GenericRecord] = {
    val userSchema: String = """{"type":"record",""" +
      """"name":"myrecord",""" +
      """"fields":[{"name":"f1","type":"string"}]}""";
    val parser: Schema.Parser = new Schema.Parser()
    val schema: Schema = parser.parse(userSchema)
    val avroRecord: GenericData.Record = new GenericData.Record(schema)
    avroRecord.put("f1", "value1")
    (_: Session) => Success[GenericRecord](avroRecord)
  }

  private def getKafkaProducer: KafkaProducer[String, GenericRecord] = {
    new KafkaProducer[String, GenericRecord]( getKafkaProtocol.properties.asJava )
  }

  private def getKafkaProtocol: KafkaProtocol = {
    KafkaProtocol("test2", Map(ProducerConfig.ACKS_CONFIG -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
      "schema.registry.url" -> "http://localhost:8081",
      "value.converter.schema.registry.url" -> "http://localhost:8081"))
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
