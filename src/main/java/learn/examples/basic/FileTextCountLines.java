package learn.examples.basic;

import java.nio.file.Paths;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

import learn.util.Constants;
import learn.util.UtilProperties;
import lombok.extern.slf4j.Slf4j;

/**
 * Ejemplo que imprime el numero de lineas que contiene un archivo de texto <br>
 * Se usa la clase SparkContext
 * @author neri
 *
 */
@Slf4j
public class FileTextCountLines {

	public static void main(String args[]) {
		
		log.info(">>>>>>>>>>Start FileTextReaderExample");
		Properties prop = UtilProperties.getAppProperties(Constants.ENV_LOCAL);
		
		SparkConf conf = new SparkConf();
		conf.setAppName("Read txt file");
		conf.setMaster("local");

		SparkContext ctx = new SparkContext(conf);
		
		String mdFilePath = Paths.get(prop.getProperty(Constants.HOME_SPARK_2_3), "README.md").toString();
		RDD<String> rdd = ctx.textFile(mdFilePath, 2);
		
		log.info(">>>>>>>>>>Count lines: {}", rdd.count());
		log.info(">>>>>>>>>>end");
	}

}
