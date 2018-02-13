package org.springframework.cloud.stream.micrometer;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.Clock;

@Configuration
@AutoConfigureBefore(SimpleMetricsExportAutoConfiguration.class)
@AutoConfigureAfter(MetricsAutoConfiguration.class)
//@ConditionalOnBean(Clock.class)
@ConditionalOnClass(Binder.class)
@ConditionalOnProperty("spring.cloud.stream.bindings." + MetersPublisherBinding.APPLICATION_METRICS + ".destination")
@EnableConfigurationProperties(MetersPublisherProperties.class)
public class DestinationPublishingMetricsAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public MetricsPublisherConfig metricsPublisherConfig(MetersPublisherProperties metersPublisherProperties) {
		return new MetricsPublisherConfig(metersPublisherProperties);
	}

	@Bean//(destroyMethod = "stop")
	@ConditionalOnMissingBean
	public DefaultDestinationPublishingMeterRegistry defaultDestinationPublishingMeterRegistry(MetersPublisherBinding publisherBinding, MetricsPublisherConfig metricsPublisherConfig, Clock clock) {
		return new DefaultDestinationPublishingMeterRegistry(publisherBinding, metricsPublisherConfig, clock);
	}
	
	@Bean
	public BeanFactoryPostProcessor metersPublisherBindingRegistrant() {
		return new BeanFactoryPostProcessor() {		
			@Override
			public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
				RootBeanDefinition emitterBindingDefinition = new RootBeanDefinition(BindableProxyFactory.class);			
				emitterBindingDefinition.getConstructorArgumentValues().addGenericArgumentValue(MetersPublisherBinding.class);
				((DefaultListableBeanFactory)beanFactory).registerBeanDefinition(MetersPublisherBinding.class.getName(), emitterBindingDefinition);			
			}
		};
	}
}
