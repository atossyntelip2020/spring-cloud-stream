
/*
 * Copyright 2018 the original author or authors.
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

import static java.util.stream.Collectors.joining;

import java.text.DecimalFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.messaging.support.GenericMessage;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Meter.Type;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.cumulative.CumulativeCounter;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;
import io.micrometer.core.instrument.cumulative.CumulativeFunctionCounter;
import io.micrometer.core.instrument.cumulative.CumulativeFunctionTimer;
import io.micrometer.core.instrument.cumulative.CumulativeTimer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.internal.DefaultGauge;
import io.micrometer.core.instrument.internal.DefaultLongTaskTimer;
import io.micrometer.core.instrument.internal.DefaultMeter;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class DefaultDestinationPublishingMeterRegistry extends MeterRegistry implements SmartLifecycle {

	private static final Logger logger = LoggerFactory.getLogger(DefaultDestinationPublishingMeterRegistry.class);

	private final MetricsPublisherConfig metricsPublisherConfig;
	
	private final Consumer<String> metricsConsumer;
	
	private final DecimalFormat format = new DecimalFormat("#.####");

	
	private final ObjectMapper objectMapper = new ObjectMapper();

	private ScheduledFuture<?> publisher;

	public DefaultDestinationPublishingMeterRegistry(MetersPublisherBinding publisherBinding, MetricsPublisherConfig metricsPublisherConfig, Clock clock) {
		this(metricsPublisherConfig, clock, new MessageChannelPublisher(publisherBinding));
	}

	public DefaultDestinationPublishingMeterRegistry(MetricsPublisherConfig metricsPublisherConfig, Clock clock, Consumer<String> metricsConsumer) {
		super(clock);
		this.metricsPublisherConfig = metricsPublisherConfig;
		this.metricsConsumer = metricsConsumer;
	}

	@Override
	public void start() {
		start(Executors.defaultThreadFactory());
	}

	@Override
	public void stop() {
		if (publisher != null) {
			publisher.cancel(false);
			publisher = null;
		}
	}

	@Override
	public boolean isRunning() {
		return this.publisher != null;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		this.stop();
		callback.run();
	}
	
	@Override
	protected <T> Gauge newGauge(Meter.Id id, T obj, ToDoubleFunction<T> f) {
		return new DefaultGauge<>(id, obj, f);
	}

	@Override
	protected Counter newCounter(Meter.Id id) {
		return new CumulativeCounter(id);
	}

	@Override
	protected LongTaskTimer newLongTaskTimer(Meter.Id id) {
		return new DefaultLongTaskTimer(id, clock);
	}

	@Override
	protected TimeUnit getBaseTimeUnit() {
		return TimeUnit.MILLISECONDS;
	}
	
	protected void publish() {
		for (Meter meter : this.getMeters()) {
			if (meter instanceof Timer) {
				this.metricsConsumer.accept(writeTimer((Timer) meter));
			}
//			else if (meter instanceof DistributionSummary) {
//				this.metricsConsumer.accept(writeSummary((DistributionSummary) meter));
//			}
		}
	}

	private String writeTimer(Timer timer) {
        final HistogramSnapshot snapshot = timer.takeSnapshot(false);
        final Stream.Builder<Field> fields = Stream.builder();

        fields.add(new Field("sum", snapshot.total(getBaseTimeUnit())));
        fields.add(new Field("count", snapshot.count()));
        fields.add(new Field("mean", snapshot.mean(getBaseTimeUnit())));
        fields.add(new Field("upper", snapshot.max(getBaseTimeUnit())));

        for (ValueAtPercentile v : snapshot.percentileValues()) {
            fields.add(new Field(format.format(v.percentile()) + "_percentile", v.value(getBaseTimeUnit())));
        }

        return toLineProtocol(timer.getId(), "histogram", fields.build(), clock.wallTime());
    }
	
	private String toLineProtocol(Meter.Id id, String metricType, Stream<Field> fields, long time) {
        String tags = getConventionTags(id).stream()
            .map(t -> "," + t.getKey() + "=" + t.getValue())
            .collect(joining(""));

        return getConventionName(id)
            + tags + ",metric_type=" + metricType + " "
            + fields.map(Field::toString).collect(joining(","))
            + " " + time;
    }

	
	private class Field {
        final String key;
        final double value;

        private Field(String key, double value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return key + "=" + format.format(value);
        }
    }

	@Override
	protected Timer newTimer(Id id, DistributionStatisticConfig distributionStatisticConfig, PauseDetector pauseDetector) {
		return new CumulativeTimer(id, clock, distributionStatisticConfig, pauseDetector, getBaseTimeUnit());
	}

	@Override
	protected <T> FunctionTimer newFunctionTimer(Id id, T obj, ToLongFunction<T> countFunction,
			ToDoubleFunction<T> totalTimeFunction, TimeUnit totalTimeFunctionUnits) {
		return new CumulativeFunctionTimer<T>(id, obj, countFunction, totalTimeFunction, totalTimeFunctionUnits,
				getBaseTimeUnit());
	}

	@Override
	protected <T> FunctionCounter newFunctionCounter(Id id, T obj, ToDoubleFunction<T> valueFunction) {
		return new CumulativeFunctionCounter<T>(id, obj, valueFunction);
	}

	@Override
	protected Meter newMeter(Id id, Type type, Iterable<Measurement> measurements) {
		return new DefaultMeter(id, type, measurements);
	}

	@Override
	protected DistributionSummary newDistributionSummary(Id id, DistributionStatisticConfig distributionStatisticConfig) {
		return new CumulativeDistributionSummary(id, clock, distributionStatisticConfig);
	}
	
	private void start(ThreadFactory threadFactory) {
		if (publisher != null) {
			stop();
		}
		//System.out.println(metricsPublisherConfig.step().toMillis());
		publisher = Executors.newSingleThreadScheduledExecutor(threadFactory).scheduleAtFixedRate(this::publish,
				metricsPublisherConfig.step().toMillis(), metricsPublisherConfig.step().toMillis(), TimeUnit.MILLISECONDS);
	}
	
	private static final class MessageChannelPublisher implements Consumer<String> {
		
		private final MetersPublisherBinding metersPublisherBinding;
		
		MessageChannelPublisher(MetersPublisherBinding metersPublisherBinding) {
			this.metersPublisherBinding = metersPublisherBinding;
		}
		@Override
		public void accept(String metricData) {
			logger.info(metricData);
			this. metersPublisherBinding.applicationMetrics().send(new GenericMessage<String>(metricData));
		}
	}

	@Override
	protected DistributionStatisticConfig defaultHistogramConfig() {
		return DistributionStatisticConfig.builder().expiry(metricsPublisherConfig.step()).build().merge(DistributionStatisticConfig.DEFAULT);
	}
}