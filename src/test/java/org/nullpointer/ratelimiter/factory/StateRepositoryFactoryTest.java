package org.nullpointer.ratelimiter.factory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.state.StateRepositoryType;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;

import static org.junit.jupiter.api.Assertions.*;

class StateRepositoryFactoryTest {

    private StateRepositoryFactory registry;

    @BeforeEach
    void setUp() {
        registry = new StateRepositoryFactory();
    }

    @Test
    void registerAndResolve() {
        StateRepository repo = new InMemoryStateRepository();
        registry.register(StateRepositoryType.IN_MEMORY, repo);

        assertSame(repo, registry.resolve(StateRepositoryType.IN_MEMORY));
    }

    @Test
    void registerNullTypeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> registry.register(null, new InMemoryStateRepository()));
    }

    @Test
    void registerNullRepositoryThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> registry.register(StateRepositoryType.IN_MEMORY, null));
    }

    @Test
    void resolveNullTypeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> registry.resolve(null));
    }

    @Test
    void laterRegistrationOverridesPrevious() {
        StateRepository first = new InMemoryStateRepository();
        StateRepository second = new InMemoryStateRepository();

        registry.register(StateRepositoryType.IN_MEMORY, first);
        registry.register(StateRepositoryType.IN_MEMORY, second);

        assertSame(second, registry.resolve(StateRepositoryType.IN_MEMORY));
    }
}
