package org.springframework.cloud.stream.binder;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.context.config.ContextFunctionCatalogAutoConfiguration;
import org.springframework.cloud.stream.config.ContentTypeConfiguration;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.messaging.MessageListeningContainer;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import reactor.core.Disposable;

public class DestinationBinderTests {

	@Configuration
	public static class TestBinderConfiguration {
		// autoconfiguration
		@Bean
		public DestinationBinder<ConsumerProperties, ProducerProperties> myBinder(FunctionCatalog functionCatalog, FunctionInspector functionInspector,
				CompositeMessageConverterFactory converterFactory) {
			return new TestDestinationBinder(functionCatalog, functionInspector, converterFactory);
		}
	}

	@Configuration
	public static class SimpleSendReceiveConfiguration {
		@Bean
		public Function<Message<byte[]>, Message<byte[]>> echo() {
			return message -> {
				return message;
			};
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSimpleSendReceive() throws Exception {
		ApplicationContext context =
				new SpringApplication(SimpleSendReceiveConfiguration.class, ContextFunctionCatalogAutoConfiguration.class,
						ContentTypeConfiguration.class, TestBinderConfiguration.class)
				.run();
		TestDestinationBinder binder = context.getBean(TestDestinationBinder.class);
		FunctionalBinding functionalBinding = context.getBean("echo_input", FunctionalBinding.class);
		Disposable subscription = functionalBinding.subscribe();

		binder.send(new GenericMessage<byte[]>("hello".getBytes()));

		Message<byte[]> resultMessage = binder.receive(1000, TimeUnit.MILLISECONDS);

		assertEquals("hello", new String(resultMessage.getPayload(), StandardCharsets.UTF_8));
		subscription.dispose();
	}

	private static class TestDestinationBinder extends DestinationBinder<ConsumerProperties, ProducerProperties> {

		private final BlockingQueue<Message<byte[]>> inputMessages = new ArrayBlockingQueue<>(1000);

		private final BlockingQueue<Message<byte[]>> outputMessages = new ArrayBlockingQueue<>(1000);

		public void send(Message<byte[]> message) {
			inputMessages.offer(message);
		}

		public Message<byte[]> receive(long timeout, TimeUnit unit) {
			try {
				return outputMessages.poll(timeout, unit);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Thread was interupted");
			}
		}

		protected TestDestinationBinder(FunctionCatalog functionCatalog, FunctionInspector functionInspector,
				CompositeMessageConverterFactory converterFactory) {
			super(new TestProvisioningProvider(), functionCatalog, functionInspector, converterFactory);
		}

		@Override
		protected MessageListeningContainer createMessageListeningContainer(String inputDestinationName,
				ConsumerProperties consumerProperties) {
			return new MessageGeneratingContainer();
		}

		@Override
		protected Consumer<Message<?>> createOutboundConsumer(String destinationName,
				ProducerProperties producerProperties) {
			return message -> outputMessages.offer((Message<byte[]>)message);
		}

		@Override
		public ConsumerProperties getExtendedConsumerProperties(String channelName) {
			return mock(ConsumerProperties.class);
		}

		@Override
		public ProducerProperties getExtendedProducerProperties(String channelName) {
			return mock(ProducerProperties.class);
		}

		private class MessageGeneratingContainer implements MessageListeningContainer {

			private Consumer<Message<?>> messageConsumer;

			private final ExecutorService executor = Executors.newSingleThreadExecutor();

			public void start() {
				System.out.println("STARTING");
				executor.execute(new Runnable() {
					@Override
					public void run() {
						while (!Thread.currentThread().isInterrupted()) {
							try {
								Message<byte[]> inputMessage = inputMessages.poll(1000, TimeUnit.MILLISECONDS);
								if (inputMessage != null) {
									messageConsumer.accept(inputMessage);
								}
							}
							catch (InterruptedException e) {
								Thread.currentThread().interrupt();
							}
						}
					}
				});
			}

			public void setListener(Consumer<Message<?>> messageConsumer) {
				this.messageConsumer = messageConsumer;
			}

			@Override
			public void stop() {
				System.out.println("SHUTTING DOWN!");
				executor.shutdownNow();
			}

			@Override
			public boolean isRunning() {
				return true;
			}
		}

		@Override
		protected void onCommit(Message<?> message) {
			// noop
		}
	}

	private static class TestProvisioningProvider implements ProvisioningProvider<ConsumerProperties, ProducerProperties> {

		@Override
		public ProducerDestination provisionProducerDestination(String name, ProducerProperties properties)
				throws ProvisioningException {
			return new ProducerDestination() {

				@Override
				public String getNameForPartition(int partition) {
					return name;
				}

				@Override
				public String getName() {
					return name;
				}
			};
		}

		@Override
		public ConsumerDestination provisionConsumerDestination(String name, String group,
				ConsumerProperties properties) throws ProvisioningException {
			return new ConsumerDestination() {
				@Override
				public String getName() {
					return name;
				}
			};
		}
	}
}
