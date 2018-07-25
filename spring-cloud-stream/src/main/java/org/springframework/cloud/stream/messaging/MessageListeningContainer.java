package org.springframework.cloud.stream.messaging;

import java.util.function.Consumer;

import org.springframework.context.SmartLifecycle;
import org.springframework.messaging.Message;

public interface MessageListeningContainer extends SmartLifecycle {

	void setListener(Consumer<Message<?>> messageConsumer);

	default boolean isAutoStartup() {
		return true;
	}

	@Override
	default int getPhase() {
		return 0;
	}

	@Override
	default void stop(Runnable callback) {
		callback.run();
		this.stop();
	}
}
