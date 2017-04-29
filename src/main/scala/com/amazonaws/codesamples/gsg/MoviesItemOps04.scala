package com.amazonaws.codesamples.gsg

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.model.ReturnValue

object MoviesItemOps04 {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val client = AmazonDynamoDBClientBuilder.standard.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2")).build
    val dynamoDB = new DynamoDB(client)
    val table = dynamoDB.getTable("Movies")
    val year = 2015
    val title = "The Big New Movie"
    val updateItemSpec = new UpdateItemSpec().
        withPrimaryKey("year", year, "title", title).
        withUpdateExpression("set info.rating = info.rating + :val").
        withValueMap(new ValueMap().withNumber(":val", 1)).
        withReturnValues(ReturnValue.UPDATED_NEW)
    try {
      System.out.println("Incrementing an atomic counter...")
      val outcome = table.updateItem(updateItemSpec)
      System.out.println("UpdateItem succeeded:\n" + outcome.getItem.toJSONPretty)
    } catch {
      case e: Exception =>
        System.err.println("Unable to update item: " + year + " " + title)
        System.err.println(e.getMessage)
    }
  }
}