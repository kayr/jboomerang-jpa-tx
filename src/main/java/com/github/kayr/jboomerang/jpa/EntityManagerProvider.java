package com.github.kayr.jboomerang.jpa;

import com.github.kayr.jboomerang.JBoomerang;
import com.github.kayr.jboomerang.JBoomerangFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.NoResultException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

public class EntityManagerProvider implements JBoomerang.ResourceFactory<EntityManager> {

    private static final Logger LOG = LoggerFactory.getLogger(EntityManagerProvider.class);
    private Supplier<EntityManagerFactory> entityManagerFactory;
    private JBoomerang<EntityManager> jpaResource = new JBoomerang<>(this);


    public <V> Optional<V> doInJpa(Object tenantId, JBoomerang.Propagation propagation, JBoomerangFunction<EntityManager, V> fx) {

        Objects.requireNonNull(tenantId, "tenant id cannot be null");
        try {
            V result = jpaResource.withResource(tenantId, propagation, JBoomerang.Args.none(), fx);
            return Optional.ofNullable(result);
        } catch (NoResultException ex) {
            //ignore
            return Optional.empty();
        }

    }

    public Optional<EntityManager> currentEm(Object discriminator) {
        return jpaResource.getCurrentResource(discriminator);
    }

    public EntityManager currentEmRequired(Object discriminator) {
        return currentEm(discriminator).orElseThrow(() -> new IllegalStateException("No Entity Is Currently bound to context. Please open a connection using JpaManager.callInJPa() or  JpaManager.callInTx() for write transactions"));
    }


    @Override
    public EntityManager create(Object discriminator, JBoomerang.Args args) {
        Objects.requireNonNull(discriminator, "Tenant Id Not Provided");
        return getEntityManager(discriminator.toString());
    }

    @Override
    public void close(Object discriminator, EntityManager resource) {
        resource.close();
    }

    @Override
    public void onException(Object discriminator, EntityManager resource) {
        //do nothing
    }


    private EntityManager getEntityManager(String tenantId) {
        LOG.trace("####Creating a new entity manager for mifostenant-{}", tenantId);
        if (LOG.isTraceEnabled()) {
            LOG.trace("", new Exception("Printing entity manager creator"));
        }
        EntityManagerFactory factory = entityManagerFactory.get();
        return Optional.ofNullable(factory)
                .orElseThrow(() -> new IllegalStateException("Failed to create EntityManagerFactory for tenant [" + tenantId + "]"))
                .createEntityManager();
    }


    @Override
    public String toString() {
        return "JPAResource:" + jpaResource.getCurrentResource().orElse(null);
    }

    public EntityManagerProvider setEntityManagerFactoryProvider(Supplier<EntityManagerFactory> entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;
        return this;
    }
}
