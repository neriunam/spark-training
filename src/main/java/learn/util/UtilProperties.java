package learn.util;

import java.util.Properties;

public class UtilProperties {
	
	public static Properties getAppProperties(String env) {
		Properties properties = new Properties();
		try {
			if (env == null || env.isEmpty()) {
				throw new RuntimeException("Must define environmet 'local' or 'server'");
			}
			if (env.toLowerCase().startsWith("local")) {
				properties.load(UtilProperties.class.getResourceAsStream("/local.properties"));
			}
			else if (env.toLowerCase().startsWith("server")) {
				properties.load(UtilProperties.class.getResourceAsStream("/server.properties"));
			}
			else{
				throw new RuntimeException("Incorrect environmet " + env + "must be: 'local' or 'server'");
			}
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
		return properties;
	}
}
