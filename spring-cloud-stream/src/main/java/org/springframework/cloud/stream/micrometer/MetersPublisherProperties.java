package org.springframework.cloud.stream.micrometer;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = MetersPublisherProperties.PREFIX)
class MetersPublisherProperties {
	
	public static final String PREFIX = "spring.cloud.stream.metrics";

	private String key;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
}
