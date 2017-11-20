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

import static java.util.stream.Collectors.joining;

import java.text.DecimalFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.ToDoubleFunction;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.HistogramSnapshot;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.ValueAtPercentile;
import io.micrometer.core.instrument.histogram.HistogramConfig;
import io.micrometer.core.instrument.internal.DefaultGauge;
import io.micrometer.core.instrument.internal.DefaultLongTaskTimer;
import io.micrometer.core.instrument.simple.CumulativeCounter;
import io.micrometer.core.instrument.simple.CumulativeDistributionSummary;
import io.micrometer.core.instrument.simple.CumulativeTimer;
import io.micrometer.core.instrument.step.StepRegistryConfig;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class DefaultDestinationPublishingMeterRegistry extends MeterRegistry implements SmartLifecycle {

	private static final Logger logger = LoggerFactory.getLogger(DefaultDestinationPublishingMeterRegistry.class);

	private final DecimalFormat format = new DecimalFormat("#.####");

	private final StepRegistryConfig dataflowConfig;

	private final Consumer<String> metricsConsumer;

    private ScheduledFuture<?> publisher;

    private final static class LogingPublisher implements Consumer<String> {
		@Override
		public void accept(String metricData) {
			logger.info(metricData);
		}
    }

	public DefaultDestinationPublishingMeterRegistry(StepRegistryConfig config) {
		this(config, new LogingPublisher());
	}

    public DefaultDestinationPublishingMeterRegistry(StepRegistryConfig config, Consumer<String> metricsConsumer) {
    		super(Clock.SYSTEM);
    		this.dataflowConfig = config;
    		this.metricsConsumer = metricsConsumer;
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
    protected Timer newTimer(Meter.Id id, HistogramConfig histogramConfig) {
        return new CumulativeTimer(id, clock, histogramConfig);
    }

    @Override
    protected DistributionSummary newDistributionSummary(Meter.Id id, HistogramConfig histogramConfig) {
        return new CumulativeDistributionSummary(id, clock, histogramConfig);
    }

    @Override
    protected void newMeter(Meter.Id id, Meter.Type type, Iterable<Measurement> measurements) {

    }

    @Override
    protected TimeUnit getBaseTimeUnit() {
        // TODO is this the base unit of time it expects?
        return TimeUnit.MILLISECONDS;
    }

    @Override
	public void start() {
        start(Executors.defaultThreadFactory());
    }

    public void start(ThreadFactory threadFactory) {
        if(publisher != null)
            stop();

        publisher = Executors.newSingleThreadScheduledExecutor(threadFactory)
            .scheduleAtFixedRate(this::publish, dataflowConfig.step().toMillis(), dataflowConfig.step().toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
	public void stop() {
        if(publisher != null) {
            publisher.cancel(false);
            publisher = null;
        }
    }

	protected void publish() {
		for (Meter meter : this.getMeters()) {
			if (meter instanceof Timer) {
				this.metricsConsumer.accept(writeTimer((Timer) meter));
			}
			else if (meter instanceof DistributionSummary) {
				this.metricsConsumer.accept(writeSummary((DistributionSummary) meter));
			}
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

	private String writeSummary(DistributionSummary summary) {
        final HistogramSnapshot snapshot = summary.takeSnapshot(false);
        final Stream.Builder<Field> fields = Stream.builder();

        fields.add(new Field("sum", snapshot.total()));
        fields.add(new Field("count", snapshot.count()));
        fields.add(new Field("mean", snapshot.mean()));
        fields.add(new Field("upper", snapshot.max()));

        for (ValueAtPercentile v : snapshot.percentileValues()) {
            fields.add(new Field(format.format(v.percentile()) + "_percentile", v.value()));
        }

        return toLineProtocol(summary.getId(), "histogram", fields.build(), clock.wallTime());
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

	private String toLineProtocol(Meter.Id id, String metricType, Stream<Field> fields, long time) {
        String tags = getConventionTags(id).stream()
            .map(t -> "," + t.getKey() + "=" + t.getValue())
            .collect(joining(""));

        return getConventionName(id)
            + tags + ",metric_type=" + metricType + " "
            + fields.map(Field::toString).collect(joining(","))
            + " " + time;
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
}
