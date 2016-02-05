package com.cloudera.se.spark.demo.util;

import com.cloudera.se.spark.demo.model.SimpleEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by jholoman on 2/2/16.
 */
public class DemoUtil {

  private static final Logger log = Logger.getLogger(DemoUtil.class);

  public static void main(String[] args) {
    if (args.length < 3) {
      System.out.println("USAGE: Example <BootStrapServers> <Topic> <num_records> //<useKey>");
      System.exit(1);
    }

    String bootstrapServers = args[0];
    String topic = args[1];
    Long iterations = Long.parseLong(args[2]);
    //Boolean useKey = Boolean.parseBoolean(args[3]);

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("client.id", "DemoProducer");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("acks", "all");
    props.put("retries", 3);

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

    for (int i = 0; i < iterations; i++) {
      long startTime = System.currentTimeMillis();
      SimpleEvent simpleEvent = new SimpleEvent();
        String event = getEventJson(simpleEvent);
        producer.send(new ProducerRecord<String, String>(topic, null, event), new GeneratorCallback(startTime, event));
    }
    producer.close();
  }

  public static String getEventJson(SimpleEvent event) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.writeValueAsString(event);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}

class GeneratorCallback implements Callback {
  private long startTime;

  public GeneratorCallback(Long startTime, String data) {
    this.startTime = startTime;
  }
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    long elapsedTime = System.currentTimeMillis() - startTime;
    if (metadata != null) {
      System.out.println(metadata.partition() + "-" + metadata.offset() +" complete in " + elapsedTime + " ms");
    } else {
      exception.printStackTrace();
    }
  }


}

