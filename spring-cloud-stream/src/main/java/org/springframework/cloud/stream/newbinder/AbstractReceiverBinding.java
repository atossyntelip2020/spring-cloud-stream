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

import java.util.function.Consumer;

import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.messaging.Message;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <P>
 * @param <C>
 */
public abstract class AbstractReceiverBinding<P extends ProducerProperties, C extends ConsumerProperties> extends AbstractBinding<P,C> {

	private Consumer<Message<byte[]>> receiver;

	public AbstractReceiverBinding(String name) {
		super(name);
	}

	void registerReceiver(Consumer<Message<byte[]>> receiver) {
		this.receiver = receiver;
	}

	protected Consumer<Message<byte[]>> getReceiver() {
		return this.receiver;
	}
}
