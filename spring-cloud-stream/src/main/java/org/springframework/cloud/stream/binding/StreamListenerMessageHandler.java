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

package org.springframework.cloud.stream.binding;

import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * @author Marius Bogoevici
 * @author Gary Russell
 * @since 1.2
 */
public class StreamListenerMessageHandler extends AbstractReplyProducingMessageHandler {

	private final InvocableHandlerMethod invocableHandlerMethod;

	private final boolean copyHeaders;

	private final MeterRegistry meterRegistry;

	private final Timer timer;

	StreamListenerMessageHandler(InvocableHandlerMethod invocableHandlerMethod, boolean copyHeaders,
			String[] notPropagatedHeaders, MeterRegistry meterRegistry) {
		super();
		this.invocableHandlerMethod = invocableHandlerMethod;
		this.copyHeaders = copyHeaders;
		this.setNotPropagatedHeaders(notPropagatedHeaders);
		this.meterRegistry = meterRegistry;

		String methodName = this.invocableHandlerMethod.getMethod().getName();
		String className = this.invocableHandlerMethod.getMethod().getDeclaringClass().getSimpleName();

		this.timer = this.meterRegistry.timer("StreamListener.timer", "method", methodName, "className", className);
	}

	@Override
	protected boolean shouldCopyRequestHeaders() {
		return this.copyHeaders;
	}

	public boolean isVoid() {
		return invocableHandlerMethod.isVoid();
	}

	@Override
	public void handleMessage(Message<?> message) {
		// May need to split it apart for cases where we want to care for additional metrics for errors
		this.timer.record(() -> super.handleMessage(message));
	}

	@Override
	protected Object handleRequestMessage(Message<?> requestMessage) {
		try {
			return this.invocableHandlerMethod.invoke(requestMessage);
		}
		catch (Exception e) {
			if (e instanceof MessagingException) {
				throw (MessagingException) e;
			}
			else {
				throw new MessagingException(requestMessage,
						"Exception thrown while invoking " + this.invocableHandlerMethod.getShortLogMessage(), e);
			}
		}
	}
}
