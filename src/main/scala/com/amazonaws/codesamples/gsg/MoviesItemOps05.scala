package com.amazonaws.codesamples.gsg

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.PrimaryKey
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.model.ReturnValue

object MoviesItemOps05 {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val client = AmazonDynamoDBClientBuilder.standard.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2")).build
    val dynamoDB = new DynamoDB(client)
    val table = dynamoDB.getTable("Movies")
    val year = 2015
    val title = "The Big New Movie"
    val updateItemSpec = new UpdateItemSpec().
      withPrimaryKey(new PrimaryKey("year", year, "title", title)).
      withUpdateExpression("remove info.actors[0]").
      withConditionExpression("size(info.actors) > :num").
      withValueMap(new ValueMap().withNumber(":num", 3)).
      withReturnValues(ReturnValue.UPDATED_NEW)
    // Conditional update (we expect this to fail)
    try {
      System.out.println("Attempting a conditional update...")
      val outcome = table.updateItem(updateItemSpec)
      System.out.println("UpdateItem succeeded:\n" + outcome.getItem.toJSONPretty)
    } catch {
      case e: Exception =>
        System.err.println("Unable to update item: " + year + " " + title)
        System.err.println(e.getMessage)
    }
  }
}