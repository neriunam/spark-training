package learn.examples;

import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import learn.util.Constants;
import learn.util.UtilProperties;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

@Slf4j
public class ExampleTransformation {

	public static void main(String[] args) {
		
		JavaSparkContext ctx = new JavaSparkContext("local", "transformations example");
		
		Properties prop = UtilProperties.getAppProperties(Constants.ENV_LOCAL);
		
		// Create RDD String orders_items lines
		JavaRDD<String[]> orderItems = ctx.
				textFile(prop.get(Constants.RETAIL_DB) + "/order_items").
				map(line -> line.split(","));
		
		// Create tuples from order_items lines, key=order_item_order_id
		JavaPairRDD<Integer, String[]> orderItemsTuple = orderItems.
				mapToPair(oi -> new Tuple2<Integer, String[]>(Integer.valueOf(oi[1]), oi));
		
		orderItemsTuple.take(10).forEach(oit -> {
			log.info("Key: {}, value: {}", oit._1, oit._2);
		});
		
		log.info("Num. orderItems: {}", orderItems.count());
		
		ctx.close();
		
	}
}
