package com.cloudera.se.spark.demo.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;

/**
 * Created by jholoman on 2/2/16.
 */
@JsonPropertyOrder({ "Headers", "Body"})
public class SimpleEvent {


  public Body Body;
  public Headers Headers;

  public SimpleEvent () {

    Headers = new Headers();
    Body = new Body();

    randomize(Body);
    randomize(Headers);

  }

  private static void randomize(Body body) {
    body.setBody1(RandomStringUtils.randomAlphabetic(20));
    body.setBody2(RandomStringUtils.randomAlphabetic(20));
    body.setBody3(RandomStringUtils.randomAlphabetic(20));
    body.setBody4(RandomStringUtils.randomAlphabetic(20));
  }

  private static void randomize(Headers headers) {
    headers.setHeader1(String.valueOf(RandomUtils.nextInt(4)));
    headers.setHeader2(RandomStringUtils.randomAlphabetic(5));
    headers.setHeader3(RandomStringUtils.randomAlphabetic(5));
    headers.setHeader4(RandomStringUtils.randomAlphabetic(5));
  }

}
