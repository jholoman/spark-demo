package com.cloudera.se.spark.demo.util;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.math.BigInteger;

/**
 * Created by jholoman on 12/20/15.
 */
public class DemoConsumer {


  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("USAGE: Example <ZookeeperConnect> <Topic>");
      System.exit(1);
    }

    String zoo = args[0];
    String topic = args[1];

    Properties props = new Properties();
    props.put("zookeeper.connect", zoo);
    props.put("group.id", "spark-demo");
    props.put("consumer.timeout.ms", "-1");
    props.put("zookeeper.session.timeout.ms", "2000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    ConsumerConfig config = new ConsumerConfig(props);

    ConsumerConnector c = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    topicMap.put(topic, 1);

    Decoder<String> d = new StringDecoder(new VerifiableProperties(props));
    Map<String, List<KafkaStream<String, String>>> streams = c.createMessageStreams(topicMap, d, d);

    KafkaStream<String, String> stream = streams.get(topic).get(0);

    ConsumerIterator<String, String> it = stream.iterator();
    while(it.hasNext()) {
      MessageAndMetadata<String, String> msg = it.next();
      System.out.println(msg.key() + "," + msg.message());
    }
  }
}
