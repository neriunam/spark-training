package learn.examples.retaildb;

import java.util.Date;
import java.util.Properties;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import learn.util.Constants;
import learn.util.UtilProperties;
import lombok.extern.slf4j.Slf4j;

/**
 * Ingresos diarios por id_product de ordenes completadas o cerradas ordenados por ingresos.<br>
 * Para este ejercicio se usa el api de spark DataFrame y spark sql
 * @author neri
 *
 */
@Slf4j
public class DailyProductRevenueSQL {

	public static void main(String[] args) throws AnalysisException {
		log.info("Start");
		
		Properties prop = UtilProperties.getAppProperties(Constants.ENV_LOCAL);
		
		SparkSession session = SparkSession.builder().
				appName("app").
				master("local").
				getOrCreate();
		
		Dataset<Row> ordersCSV = session.
				read().
				csv(prop.getProperty(Constants.RETAIL_DB) + "/orders/*").
				toDF("order_id", "order_date", "order_customer_id", "order_status");
		
		Dataset<Row> orderItemsCSV = session.
				read().
				csv(prop.getProperty(Constants.RETAIL_DB) + "/order_items/*").
				toDF("order_item_id", "order_item_order_id", "order_item_product_id","order_item_quantity", 
					 "order_item_subtotal", "order_item_product_price");
		
		Dataset<Row> orders = ordersCSV.
				withColumn("order_id", ordersCSV.col("order_id").cast(DataTypes.IntegerType)).
				withColumn("order_customer_id", ordersCSV.col("order_customer_id").cast(DataTypes.IntegerType));
		
		Dataset<Row> orderItems = orderItemsCSV.
			    withColumn("order_item_id", orderItemsCSV.col("order_item_id").cast(DataTypes.IntegerType)).
			    withColumn("order_item_order_id", orderItemsCSV.col("order_item_order_id").cast(DataTypes.IntegerType)).
			    withColumn("order_item_product_id", orderItemsCSV.col("order_item_product_id").cast(DataTypes.IntegerType)).
			    withColumn("order_item_quantity", orderItemsCSV.col("order_item_quantity").cast(DataTypes.IntegerType)).
			    withColumn("order_item_subtotal", orderItemsCSV.col("order_item_subtotal").cast(DataTypes.FloatType)).
			    withColumn("order_item_product_price", orderItemsCSV.col("order_item_product_price").cast(DataTypes.FloatType));
		
		orders.createTempView("orders");
		orderItems.createTempView("order_items");
		
		String sqlString = new StringBuilder().
				append("select o.order_date, oi.order_item_product_id, ").
				append("round(sum(oi.order_item_subtotal), 2) as revenue ").
				append("from orders o join order_items oi ").
				append("on o.order_id = oi.order_item_order_id "). 
				append("where o.order_status in ('COMPLETE', 'CLOSED') "). 
				append("group by o.order_date, oi.order_item_product_id ").
				append("order by o.order_date, revenue desc ").
				toString();
		
		Dataset<Row> dailyProductRevenue = session.sql(sqlString);
		
		dailyProductRevenue.show();
				
		dailyProductRevenue.
			coalesce(3).
			write().
			csv(prop.getProperty(Constants.OUTPUT_DIR) + "/daily_product_revenue_sql_" + new Date().getTime());
		
		session.close();
		log.info("End");
	}
}
