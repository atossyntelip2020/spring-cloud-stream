package org.springframework.cloud.stream.newbinder;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedBindingProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.function.FunctionInvoker;
import org.springframework.cloud.stream.function.FunctionRegistry;
import org.springframework.cloud.stream.function.TypeAwareConsumer;
import org.springframework.cloud.stream.function.TypeAwareFunction;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.Lifecycle;
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
public abstract class DestinationBinder<F, C extends ConsumerProperties, P extends ProducerProperties> implements
		InitializingBean, ApplicationContextAware, ExtendedBindingProperties<C, P> {

	protected final Log logger = LogFactory.getLog(this.getClass());

	private final ProvisioningProvider<C, P> provisioningProvider;

	private final FunctionRegistry<F> functionRegistry;

	private ConfigurableApplicationContext applicationContext;

	public DestinationBinder(ProvisioningProvider<C, P> provisioningProvider, FunctionRegistry<F> functionRegistry) {
		this.provisioningProvider = provisioningProvider;
		this.functionRegistry = functionRegistry;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void afterPropertiesSet() throws Exception {
		if (!CollectionUtils.isEmpty(this.functionRegistry.getFunctions())) {
			for (Entry<String, TypeAwareFunction> functionEntry : this.functionRegistry.getFunctions().entrySet()) {
				String functionName = functionEntry.getKey();
				String inputBindingName = functionName + "_input";
				String outputBindingName = functionName + "_output";
				String groupName = "";

				ProducerDestination producerDestination = this.provisioningProvider
						.provisionProducerDestination(outputBindingName, this.getExtendedProducerProperties(outputBindingName));
				// Consumer that will handle sending to OUTPUT Destination (i.e., Outbound Adapter)
				Consumer<F> producingConsumer = this.bindProducer(producerDestination.getName(), this.getExtendedProducerProperties(outputBindingName));

				FunctionInvoker<F> functionInvoker = this.functionRegistry.getFunctionInvoker(functionEntry.getValue(), producingConsumer);

				ConsumerDestination consumerDestination = this.provisioningProvider
						.provisionConsumerDestination(inputBindingName, groupName, this.getExtendedConsumerProperties(inputBindingName));
				Lifecycle listeningContainer = this.bindConsumer(consumerDestination.getName(), this.getExtendedConsumerProperties(inputBindingName), functionInvoker);

				ConsumerBinding<F> consumerBinding = new ConsumerBinding<>(consumerDestination, listeningContainer);
				ProducerBinding<F> producerBinding = new ProducerBinding<>(producerDestination, producingConsumer);
				consumerBinding.setProducerBinding(producerBinding); // so we can start it as a single biniding

				this.registerBinding(consumerBinding);
			}
		}
		else if (!CollectionUtils.isEmpty(this.functionRegistry.getConsumers())) {
			for (Entry<String, TypeAwareConsumer> consumerEntry : this.functionRegistry.getConsumers().entrySet()) {
				String functionName = consumerEntry.getKey();
				String inputBindingName = functionName + "_input";
				String groupName = "";

				ConsumerDestination consumerDestination = this.provisioningProvider
						.provisionConsumerDestination(inputBindingName, groupName, this.getExtendedConsumerProperties(inputBindingName));

				FunctionInvoker<F> functionInvoker = this.functionRegistry.getConsumerInvoker(consumerEntry.getValue());

				Lifecycle listeningContainer = this.bindConsumer(consumerDestination.getName(), this.getExtendedConsumerProperties(inputBindingName), functionInvoker);
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

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	protected ConfigurableApplicationContext getApplicationContext() {
		return applicationContext;
	}

	protected abstract Lifecycle bindConsumer(String destinationName, C consumerProperties, FunctionInvoker<F> functionInvoker);

	protected abstract Consumer<F> bindProducer(String destinationName, P producerProperties);

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
