package com.amazonaws.codesamples.gsg

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.document.utils.NameMap
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap

object MoviesScan {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val client = AmazonDynamoDBClientBuilder.standard.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2")).build
    val dynamoDB = new DynamoDB(client)
    val table = dynamoDB.getTable("Movies")
    val scanSpec = new ScanSpec().
      withProjectionExpression("#yr, title, info.rating").
      withFilterExpression("#yr between :start_yr and :end_yr").
      withNameMap(new NameMap().`with`("#yr", "year")).
      withValueMap(new ValueMap().
      withNumber(":start_yr", 1950).
      withNumber(":end_yr", 1959))
    try {
      val items = table.scan(scanSpec)
      val iter = items.iterator
      while ( {
        iter.hasNext
      }) {
        val item = iter.next
        System.out.println(item.toString)
      }
    } catch {
      case e: Exception =>
        System.err.println("Unable to scan the table:")
        System.err.println(e.getMessage)
    }
  }
}