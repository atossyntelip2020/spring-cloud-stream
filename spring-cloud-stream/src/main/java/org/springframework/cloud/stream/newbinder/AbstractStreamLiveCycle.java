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
package org.springframework.cloud.stream.newbinder;

import org.springframework.context.SmartLifecycle;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

/**
 *
 * @author Oleg Zhurakousky
 */
@ManagedResource
public abstract class AbstractStreamLiveCycle implements SmartLifecycle {

	private boolean running;

	public abstract void doStart();

	public abstract void doStop();

	@Override
	@ManagedOperation
	public synchronized void start() {
		if (!this.running) {
			this.doStart();
		}
		this.running = true;
	}

	@Override
	@ManagedOperation
	public synchronized void stop() {
		if (this.running) {
			this.doStop();
		}
		this.running = false;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	public boolean isAutoStartup() {
		return false;
	}

	@Override
	public synchronized void stop(Runnable callback) {
		if (callback != null) {
			callback.run();
		}
		this.stop();
	}
}
