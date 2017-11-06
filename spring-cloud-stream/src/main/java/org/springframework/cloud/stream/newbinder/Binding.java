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
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.context.SmartLifecycle;

/**
 * <p>
 * Binding is a bridge between the source system and functional interface.
 * The instance of this strategy represents the active bridge delegating
 * to/from source/target system and functional interfaces (i.e., {@link Supplier},
 * {@link Function}, {@link Consumer}) exposed as beans by the application.
 * </p>
 * <p>
 * Implementations are responsible to handle both content negotiation and
 * type conversion to adapt data coming <i>from</i> the source system or going
 * <i>to</i> the target system.
 * </p>
 * @see AbstractSenderBinding
 * @see AbstractReceiverBinding
 *
 * @author Oleg Zhurakousky
 *
 */
interface Binding extends SmartLifecycle {

	default void suspend() {
		this.stop();
	}

	default void resume() {
		this.start();
	}

	String getName();
}
