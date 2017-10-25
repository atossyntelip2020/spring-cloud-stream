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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.util.Assert;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <P>
 * @param <C>
 */
abstract class AbstractBinding<P extends ProducerProperties, C extends ConsumerProperties> extends AbstractStreamLiveCycle implements Binding {

	private final String name;

	@Autowired
	private P producerProperties;

	@Autowired
	private C consumerProperties;

	@Autowired
	private ProvisioningProvider<C, P> provisioningProvider;

	public AbstractBinding(String name) {
		Assert.hasText(name, "'name' must not be null or empty.");
		this.name = name;
	}

	@Override
	public String getName() {
		return this.name;
	}

	protected P getProducerProperties() {
		return this.producerProperties;
	}

	protected C getConsumerProperies() {
		return this.consumerProperties;
	}

	protected ProvisioningProvider<C, P> getProvisioningProvider() {
		return this.provisioningProvider;
	}
}
