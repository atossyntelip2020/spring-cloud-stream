/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cloud.stream.micrometer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.actuate.autoconfigure.metrics.export.MetricsExporter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.support.management.AbstractMessageHandlerMetrics;
import org.springframework.integration.support.management.DefaultMessageHandlerMetrics;
import org.springframework.integration.support.management.DefaultMetricsFactory;
import org.springframework.integration.support.management.IntegrationManagementConfigurer;
import org.springframework.integration.support.management.MetricsContext;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.instrument.step.StepRegistryConfig;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
@Configuration
public class MicrometerMetricsConfig {

	/**
	 *
	 */
	@Bean
    public MetricsExporter publishingMeterRegistryExporter() {
		StepRegistryConfig config = new StepRegistryConfig() {

			@Override
			public String prefix() {
				return "spring.cloud.stream.metric";
			}

			@Override
			public String get(String key) {
				return null; // will use the default except for overriden  step()
			}

			@Override
			public Duration step() {
				// TODO Auto-generated method stub
				return Duration.ofSeconds(5);
			}
		};
		DefaultDestinationPublishingMeterRegistry registry = new DefaultDestinationPublishingMeterRegistry(config);
		registry.start();
        return () -> registry;
    }

	/**
	 * Configures {@link MeterRegistryAwareDefaultMessageHandlerMetrics} to provide seamless
	 *  integration with current SI metrics gathering mechanisms.
	 *
	 */
	@Bean
	public IntegrationManagementConfigurer integrationManagementConfigurer(MeterRegistry meterRegistry) {
		IntegrationManagementConfigurer configurer = new IntegrationManagementConfigurer();
		configurer.setDefaultCountsEnabled(true);
		DefaultMetricsFactory metricsFactory = new DefaultMetricsFactory() {
			@Override
			public AbstractMessageHandlerMetrics createHandlerMetrics(String name) {
				return new MeterRegistryAwareDefaultMessageHandlerMetrics(name, meterRegistry);
			}
		};
		configurer.setMetricsFactory(metricsFactory);
		return configurer;
	}

	/**
	 * Implementation of {@link DefaultMessageHandlerMetrics} which is
	 * aware of the {@link MeterRegistry}.
	 *
	 * With introduction of {@link MeterRegistry} somr if not all of the
	 * functionality provided by the {@link DefaultMessageHandlerMetrics} may be redundant,
	 * since most of the counters and dimensions are already available (or could be made available)
	 * through Micrometer meters.
	 * So, the primary purpose of this class is to provide transparent
	 * integration with Spring Integration's current metrics gathering infrastructure while exposing
	 * a path for incremental migration to full Micrometer support.
	 *
	 */
	private static class MeterRegistryAwareDefaultMessageHandlerMetrics extends DefaultMessageHandlerMetrics {

		private final Timer timer;

		private final Clock clock = Clock.SYSTEM;

		public MeterRegistryAwareDefaultMessageHandlerMetrics(String name, MeterRegistry meterRegistry) {
			super(name);
			this.timer = meterRegistry.timer(name);
		}

		@Override
		public MetricsContext beforeHandle() {
			long start = clock.monotonicTime();
			this.handleCount.incrementAndGet();
			this.activeCount.incrementAndGet();
			return new StreamHandlerMetricsContext(start);
		}

		@Override
		public void afterHandle(MetricsContext context, boolean success) {
			long elapsedTime = clock.monotonicTime() - ((StreamHandlerMetricsContext) context).getStart();

			timer.record(elapsedTime, TimeUnit.NANOSECONDS);
			this.activeCount.decrementAndGet();
			if (isFullStatsEnabled() && success) {
				this.duration.append(System.nanoTime() - ((StreamHandlerMetricsContext) context).getStart());
			}
			else if (!success) {
				this.errorCount.incrementAndGet();
			}
		}

		static class StreamHandlerMetricsContext extends DefaultHandlerMetricsContext {
			private StreamHandlerMetricsContext(long start) {
				super(start);
			}
			long getStart() {
				return this.start;
			}
		}
	}
}
