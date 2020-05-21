package learn.examples.retaildb;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import learn.util.Constants;
import learn.util.UtilProperties;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

/**
 * Ingresos por dia para ordenes completadas o cerradas ordenadas por fecha<br>
 * Para este ejercicio se usa unicamente RDD api
 * @author neri
 *
 */
@Slf4j
public class DailyRevenueRDD {

	public static void main(String[] args) {
		log.info("Start");
		
		JavaSparkContext ctx = new JavaSparkContext("local", "app");
		
		Properties prop = UtilProperties.getAppProperties(Constants.ENV_LOCAL);
		
		List<String> listEstatusFilter = Arrays.asList("COMPLETE", "CLOSED");
		
		Function<String, String[]> splitFunction = s -> s.split(",");
		
		JavaRDD<String[]> orders = ctx.textFile(prop.getProperty(Constants.RETAIL_DB) + "/orders/*").
				map(splitFunction);
		JavaRDD<String[]> orderItems = ctx.textFile(prop.getProperty(Constants.RETAIL_DB) + "/order_items/*").
				map(splitFunction);
		
		JavaRDD<String[]> ordersFiltered = orders.filter(row -> listEstatusFilter.contains(row[3]));
		
		// Create tuple (order_id, order_date)
		JavaPairRDD<Integer, String> ordersFilteredMap = ordersFiltered.mapToPair(row -> new Tuple2<>(Integer.valueOf(row[0]), row[1]));
		
		JavaPairRDD<Integer, Float> orderItemsMap = orderItems.mapToPair(row -> new Tuple2<>(Integer.valueOf(row[1]), Float.valueOf(row[4])));
		                          
		JavaPairRDD<Integer, Tuple2<String, Float>> ordersJoin = ordersFilteredMap.join(orderItemsMap);
		
		JavaPairRDD<String, Float> ordersJoinMap = ordersJoin.mapToPair(o -> new Tuple2<>(o._2._1, o._2._2));
		
		JavaPairRDD<String, Float> dailyRevenue = ordersJoinMap.reduceByKey((x, y) -> x + y);
		                                            
		JavaPairRDD<String, Float> dailyRevenueSorted = dailyRevenue.sortByKey();
		
		JavaRDD<String> dailyRevenueSortedMap = dailyRevenueSorted.map(o -> o._1 + "," + o._2);
		
		dailyRevenueSortedMap.take(10).forEach(System.out::println);
		
		dailyRevenueSortedMap.saveAsTextFile(prop.getProperty(Constants.OUTPUT_DIR) + "/daily_revenue_rdd_" + new Date().getTime());
		
		
		ctx.close();
		log.info("End");
	}
}
