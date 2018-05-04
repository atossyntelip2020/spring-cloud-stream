package org.springframework.cloud.stream.newbinder;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedBindingProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.function.TypeAwareFunction;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.util.CollectionUtils;

/**
 * Binder which binds functions directly to destinations.
 *
 * @author Oleg Zhurakousky
 *
 * @param <C> - the type of {@link ExtendedConsumerProperties}
 * @param <P> - the type of {@link ExtendedProducerProperties}
 * @param <F> - the type of raw input of a function. For example Message or Flux etc.
 */
public abstract class DestinationBinder<C extends ConsumerProperties, P extends ProducerProperties> implements
		InitializingBean, ApplicationContextAware, ExtendedBindingProperties<C, P> {

	protected final Log logger = LogFactory.getLog(this.getClass());

	private final ProvisioningProvider<C, P> provisioningProvider;

	private ConfigurableApplicationContext applicationContext;

	private final FunctionCatalog functionCatalog;

	private final FunctionInspector functionInspector;

	private final SmartMessageConverter messageConverter;

	/**
	 *
	 * @param provisioningProvider
	 * @param functionCatalog
	 * @param functionInspector
	 * @param converterFactory
	 * @param defaultRoute
	 * @param share
	 */
	public DestinationBinder(ProvisioningProvider<C, P> provisioningProvider, FunctionCatalog functionCatalog,
			FunctionInspector functionInspector, CompositeMessageConverterFactory converterFactory) {
		this.messageConverter = converterFactory != null ? converterFactory.getMessageConverterForAllRegistered() : null;
		this.provisioningProvider = provisioningProvider;
		this.functionCatalog = functionCatalog;
		this.functionInspector = functionInspector;
	}

	/**
	 *
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void afterPropertiesSet() throws Exception {
		Set<String> functions = functionCatalog.getNames(Function.class);
		Set<String> consumers = functionCatalog.getNames(Consumer.class);
		if (!CollectionUtils.isEmpty(functions)) {
			for (String functionName : functions) {
				Object userFunction = functionCatalog.lookup(Function.class, functionName);

				String inputBindingName = functionName + "_input";
				String outputBindingName = functionName + "_output";
				String groupName = "hello";

				ProducerDestination producerDestination = this.provisioningProvider
						.provisionProducerDestination(outputBindingName, this.getExtendedProducerProperties(outputBindingName));
				Consumer<Message<?>> outboundConsumer = this.createOutboundConsumer(producerDestination.getName(), this.getExtendedProducerProperties(outputBindingName));

				TypeAwareFunction messageHandler = new TypeAwareFunction(userFunction, this.functionInspector, this.messageConverter, outboundConsumer);

				ConsumerDestination consumerDestination = this.provisioningProvider.provisionConsumerDestination(
						inputBindingName, groupName, this.getExtendedConsumerProperties(inputBindingName));
				Object inboundListener = this.createInboundListener(consumerDestination.getName(), this.getExtendedConsumerProperties(inputBindingName), messageHandler);

				ConsumerBinding consumerBinding = new ConsumerBinding<>(consumerDestination, inboundListener);
				ProducerBinding producerBinding = new ProducerBinding<>(producerDestination, outboundConsumer);
				consumerBinding.setProducerBinding(producerBinding); // so we can start it as a single binding
				this.registerBinding(consumerBinding);
			}
		}
		else if (!CollectionUtils.isEmpty(consumers)) {
			for (String consumerName : consumers) {
				Consumer consumer = functionCatalog.lookup(Consumer.class, consumerName);
				String inputBindingName = consumerName + "_input";
				String groupName = "hello";

				ConsumerDestination consumerDestination = this.provisioningProvider
						.provisionConsumerDestination(inputBindingName, groupName, this.getExtendedConsumerProperties(inputBindingName));

				TypeAwareFunction messageHandler = new TypeAwareFunction(consumer, this.functionInspector, this.messageConverter);

				Lifecycle listeningContainer = this.createInboundListener(consumerDestination.getName(), this.getExtendedConsumerProperties(inputBindingName), messageHandler);
				ConsumerBinding consumerBinding = new ConsumerBinding(consumerDestination, listeningContainer);
				this.registerBinding(consumerBinding);
			}
		}
		// TODO This may need to be moved to Lifecycle start. This way all bindings can be controlled over a single call as well as individual bindings.
		Map<String, Binding> bindings = this.applicationContext.getBeansOfType(Binding.class);
		for (Entry<String, Binding> bindingEntry : bindings.entrySet()) {
			System.out.println("Starting binding: " + bindingEntry.getKey());
			bindingEntry.getValue().start();
		}
	}

	/**
	 *
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	/**
	 *
	 * @param destinationName
	 * @param consumerProperties
	 * @param typeAware
	 * @return
	 */
	protected abstract Lifecycle createInboundListener(String destinationName, C consumerProperties, TypeAwareFunction typeAware);

	/**
	 *
	 * @param destinationName
	 * @param producerProperties
	 * @return
	 */
	protected abstract Consumer<Message<?>> createOutboundConsumer(String destinationName, P producerProperties);

	/**
	 *
	 * @return
	 */
	protected ConfigurableApplicationContext getApplicationContext() {
		return applicationContext;
	}

	/**
	 *
	 */
	private void registerBinding(Binding<?> binding) {
		BeanFactory bf = this.applicationContext.getBeanFactory();
		if (bf instanceof ConfigurableListableBeanFactory) {
			((ConfigurableListableBeanFactory)bf).registerSingleton(binding.getName(), binding);
		}
		if (bf instanceof AutowireCapableBeanFactory) {
			((AutowireCapableBeanFactory)bf).initializeBean(binding, binding.getName());
		} // TODO warn if not autowirable
	}
}
