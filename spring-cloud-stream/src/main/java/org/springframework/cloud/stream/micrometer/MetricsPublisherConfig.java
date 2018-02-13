package org.springframework.cloud.stream.micrometer;

import io.micrometer.core.instrument.step.StepRegistryConfig;

class MetricsPublisherConfig implements StepRegistryConfig {
	
	private final MetersPublisherProperties metersPublisherProperties;

	public MetricsPublisherConfig(MetersPublisherProperties metersPublisherProperties) {
		this.metersPublisherProperties = metersPublisherProperties;
	}

	@Override
	public String prefix() {
		return MetersPublisherProperties.PREFIX;
	}

	@Override
	public String get(String key) {
		return null; // will use the default except for overriden  step()
	}
}
