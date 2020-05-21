package learn.examples.basic;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ParallelizeExample {

	public static void main(String[] args) {

		JavaSparkContext sc = new JavaSparkContext("local", "appname");
		List<Integer> numList = Arrays.asList(1, 2, 3, 4, 5);

		JavaRDD<Integer> rddIntegers = sc.parallelize(numList);

		// collect() method should only be used if the resulting array is expected to be
		// small, asall the data is loaded into the driver's memory.
		rddIntegers.collect().forEach(System.out::println);
		sc.close();
	}

}
