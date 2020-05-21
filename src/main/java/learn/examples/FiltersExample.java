package learn.examples;

import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import learn.util.Constants;
import learn.util.UtilProperties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FiltersExample {
	
	private static final String CLOSED = "CLOSED";
	
	public static void main(String args[]) {
		
		log.info("Start spark filters example");
		
		JavaSparkContext ctx = new JavaSparkContext("local", "Filter examples");
		
		Properties prop = UtilProperties.getAppProperties(Constants.ENV_LOCAL);
		
		// Create RDD String orders lines
		JavaRDD<String> orders = ctx.textFile(prop.get(Constants.RETAIL_DB) + "/orders");
		
		log.info("Num. orders: {}", orders.count());
		
		// Filter by orders status
		JavaRDD<String> ordersClosed = orders.filter(line -> line.split(",")[3].equals(CLOSED));
		JavaRDD<String> ordersNotClosed = orders.filter(line -> !line.split(",")[3].equals(CLOSED));
		
		log.info("Num. orders closed: {}", ordersClosed.count());
		log.info("Num. orders not closed: {}", ordersNotClosed.count());
		
		// Filter by orders status closed and period 2013-05
		Function<String, Boolean> filter = line -> line.split(",")[1].startsWith("2013-07") && 
				                                   line.split(",")[3].equals(CLOSED);
		JavaRDD<String> ordersFiltered = orders.filter(filter);
		log.info("Num. orders closed with period 2013-07: {}", ordersFiltered.count());
		
		ordersFiltered.take(20).forEach(System.out::println);
		
		ctx.close();
		log.info("End");
	}
}
