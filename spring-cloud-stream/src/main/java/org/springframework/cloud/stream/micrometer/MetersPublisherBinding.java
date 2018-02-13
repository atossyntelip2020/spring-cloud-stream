package org.springframework.cloud.stream.micrometer;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface MetersPublisherBinding {

	String APPLICATION_METRICS = "applicationMetrics";

	@Output(APPLICATION_METRICS)
	MessageChannel applicationMetrics();
}
