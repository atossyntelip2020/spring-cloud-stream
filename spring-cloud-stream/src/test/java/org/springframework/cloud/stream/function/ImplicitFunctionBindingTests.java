/*
 * Copyright 2019-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.function;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Test;
import reactor.core.publisher.Flux;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class ImplicitFunctionBindingTests {

	@After
	public void after() {
		System.clearProperty("spring.cloud.stream.function.definition");
		System.clearProperty("spring.cloud.function.definition");
	}

	@Test
	public void testEmptyConfiguration() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						EmptyConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false", "--debug")) {
			context.getBean(InputDestination.class);
		}
		catch (Exception e) { // should not fail
			fail();
		}
	}

	@Test
	public void testSimpleFunctionWithStreamProperty() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						NoEnableBindingConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.stream.function.definition=func")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());

		}
	}

	@Test
	public void testSimpleFunctionWithNativeProperty() {

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						NoEnableBindingConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.function.definition=func")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());

		}
	}

	@Test
	public void testSimpleFunctionWithoutDefinitionProperty() {
		System.clearProperty("spring.cloud.stream.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						SingleFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());

		}
	}

	@Test
	public void testSimpleConsumerWithoutDefinitionProperty() {
		System.clearProperty("spring.cloud.stream.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						SingleConsumerConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage);

			assertThat(System.getProperty("consumer")).isEqualTo("Hello");
			System.clearProperty("consumer");
		}
	}

	@Test
	public void testConsumer() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration
						.getCompleteConfiguration(SingleConsumerConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.cloud.stream.function.definition=consumer",
										"--spring.jmx.enabled=false",
										"--spring.cloud.stream.bindings.input.content-type=text/plain",
										"--spring.cloud.stream.bindings.input.consumer.use-native-decoding=true")) {

			InputDestination source = context.getBean(InputDestination.class);
			source.send(new GenericMessage<byte[]>("John Doe".getBytes()));
		}
	}

	@Test
	public void testBindingWithReactiveFunction() {
		System.clearProperty("spring.cloud.stream.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ReactiveFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessageOne = MessageBuilder
					.withPayload("Hello".getBytes()).build();
			Message<byte[]> inputMessageTwo = MessageBuilder
					.withPayload("Hello Again".getBytes()).build();
			inputDestination.send(inputMessageOne);
			inputDestination.send(inputMessageTwo);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());
			outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello Again".getBytes());
		}
	}

	@Test
	public void testFunctionConfigDisabledIfStreamListenerIsUsed() {
		System.clearProperty("spring.cloud.stream.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						LegacyConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false")) {

			assertThat(context.getBean("supplierInitializer")).isEqualTo(null);
		}
	}


	@Test(expected = Exception.class)
	public void testDeclaredTypeVsActualInstance() {
		System.clearProperty("spring.cloud.stream.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						SCF_GH_409Configuration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);

			Message<byte[]> inputMessageOne = MessageBuilder
					.withPayload("Hello".getBytes()).build();

			inputDestination.send(inputMessageOne);
		}
	}

	@Test
	public void testWithContextTypeApplicationProperty() {
		System.clearProperty("spring.cloud.stream.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						SingleFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.stream.bindings.input.content-type=text/plain")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessageOne = MessageBuilder
					.withPayload("Hello".getBytes()).build();
			Message<byte[]> inputMessageTwo = MessageBuilder
					.withPayload("Hello Again".getBytes()).build();
			inputDestination.send(inputMessageOne);
			inputDestination.send(inputMessageTwo);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello".getBytes());
			outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Hello Again".getBytes());
		}
	}

	@Test
	public void testWithIntegrationFlowAsFunction() {
		System.clearProperty("spring.cloud.stream.function.definition");
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						FunctionSampleSpringIntegrationConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("hello".getBytes()).build();

			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("HELLO".getBytes());
		}
	}

	@EnableAutoConfiguration
	public static class NoEnableBindingConfiguration  {

		@Bean
		public Function<String, String> func() {
			return x -> {
				System.out.println("Function");
				return x;
			};
		}

		@Bean
		public Consumer<String> cons() {
			return x -> {
				System.out.println("Consumer");
			};
		}
	}

	@EnableAutoConfiguration
	public static class SingleFunctionConfiguration {

		@Bean
		public Function<String, String> func() {
			return x -> {
				System.out.println("Function");
				return x;
			};
		}
	}

	@EnableAutoConfiguration
	public static class SingleConsumerConfiguration {

		@Bean
		public Consumer<String> consumer() {
			return value -> {
				System.out.println(value);
				System.setProperty("consumer", value);
			};
		}
	}

	@EnableAutoConfiguration
	public static class ReactiveFunctionConfiguration {

		@Bean
		public Function<Flux<String>, Flux<String>> echo() {
			return flux -> flux.map(value -> {
				System.out.println("echo value reqctive " + value);
				return value;
			});
		}
	}

	@EnableAutoConfiguration
	@EnableBinding(Sink.class)
	public static class LegacyConfiguration {

		@StreamListener(Sink.INPUT)
		public void handle(String value) {

		}
	}

	@EnableAutoConfiguration
	public static class EmptyConfiguration {

	}

	@EnableAutoConfiguration
	public static class SCF_GH_409Configuration {

		@Bean
		public Serializable blah() {
			return new Foo();
		}

		private static class Foo implements Supplier<Object>, Serializable {

			@Override
			public Object get() {
				// TODO Auto-generated method stub
				return null;
			}

		}
	}

	@EnableAutoConfiguration
	public static class FunctionSampleSpringIntegrationConfiguration {

		@Bean
		public IntegrationFlow uppercaseFlow() {
			return IntegrationFlows.from(MessageFunction.class, "uppercase")
					.<String, String>transform(String::toUpperCase)
					.logAndReply(LoggingHandler.Level.WARN);
		}

	}

	public interface MessageFunction extends Function<Message<String>, Message<String>> {

	}

}
