package com.amazonaws.codesamples.gsg

import java.util
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec

object MoviesQuery {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val client = AmazonDynamoDBClientBuilder.standard.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2")).build
    val dynamoDB = new DynamoDB(client)
    val table = dynamoDB.getTable("Movies")
    val nameMap = new util.HashMap[String, String]
    nameMap.put("#yr", "year")
    val valueMap = new util.HashMap[String, Object]
    valueMap.put(":yyyy", new java.lang.Integer(1985))
    val querySpec = new QuerySpec().withKeyConditionExpression("#yr = :yyyy").withNameMap(nameMap).withValueMap(valueMap)
    try {
      System.out.println("Movies from 1985")
      val items = table.query(querySpec)
      val iterator = items.iterator
      while ( {
        iterator.hasNext
      }) {
        val item = iterator.next
        System.out.println(item.getNumber("year") + ": " + item.getString("title"))
      }
    } catch {
      case e: Exception =>
        System.err.println("Unable to query movies from 1985")
        System.err.println(e.getMessage)
    }
    valueMap.put(":yyyy",  new java.lang.Integer(1992))
    valueMap.put(":letter1", "A")
    valueMap.put(":letter2", "L")
    querySpec.withProjectionExpression("#yr, title, info.genres, info.actors[0]").withKeyConditionExpression("#yr = :yyyy and title between :letter1 and :letter2").withNameMap(nameMap).withValueMap(valueMap)
    try {
      System.out.println("Movies from 1992 - titles A-L, with genres and lead actor")
      val items = table.query(querySpec)
      val iterator = items.iterator
      while ( {
        iterator.hasNext
      }) {
        val item = iterator.next
        System.out.println(item.getNumber("year") + ": " + item.getString("title") + " " + item.getMap("info"))
      }
    } catch {
      case e: Exception =>
        System.err.println("Unable to query movies from 1992:")
        System.err.println(e.getMessage)
    }
  }
}
