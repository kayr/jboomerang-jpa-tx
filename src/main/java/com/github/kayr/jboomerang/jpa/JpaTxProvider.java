package com.github.kayr.jboomerang.jpa;

import com.github.kayr.jboomerang.BoomerangCloseException;
import com.github.kayr.jboomerang.JBoomerang;
import com.github.kayr.jboomerang.JBoomerangFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityTransaction;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Generic Functional Parameter that can run a single job in one transaction and
 * rollback the transaction if an error occurs.
 * <p>
 */
public class JpaTxProvider implements JBoomerang.ResourceFactory<TxHolder> {

    private static final Logger LOG = LoggerFactory.getLogger(JpaTxProvider.class);

    private final JBoomerang<TxHolder> txResource = new JBoomerang<>(this);


    private EntityManagerProvider entityManagerProvider;

    public JpaTxProvider(EntityManagerProvider entityManagerProvider) {
        this.entityManagerProvider = entityManagerProvider;
    }


    /**
     * Helper method so that we do not use the jpa session manager in the connectors
     */
    public void doInTX(WorkUnit fx) {
        doInTX(JBoomerang.Propagation.JOIN, fx);
    }

    public void doInTX(JBoomerang.Propagation propagation, WorkUnit fx) {
        doInTX(txResource.currentDiscriminatorOrCommon(), propagation, t -> {
            fx.run();
            return Void.TYPE;
        });
    }


    /**
     * Helper method so that we do not use the jpa session manager in the connectors
     */
    public <V> Optional<V> callInTX(Callable<V> fx) {
        return callInTX(JBoomerang.Propagation.JOIN, fx);
    }

    public <V> Optional<V> callInTX(JBoomerang.Propagation propagation, Callable<V> fx) {
        return doInTX(txResource.currentDiscriminatorOrCommon(), propagation, th -> fx.call());
    }

    public TxHolder current() {
        return txResource.getCurrentResource().orElseThrow(() -> new IllegalStateException("no current open transaction for : " + txResource.currentDiscriminator()));
    }

    public TxHolder current(Object discriminator) {
        return txResource.getCurrentResource(discriminator)
                         .orElseThrow(() -> new IllegalStateException("no current open transaction for : " + discriminator));
    }



    public <V> Optional<V> doInTX(Object discriminator, JBoomerang.Propagation propagation, JBoomerangFunction<TxHolder, V> fx) {
        Objects.requireNonNull(discriminator, "tenant id cannot be null");
        return entityManagerProvider.doInJpa(discriminator, propagation, em -> txResource.withResource(discriminator, propagation, JBoomerang.Args.of(em), fx));


    }


    @Override
    public TxHolder create(Object discriminator, JBoomerang.Args args) {
        EntityManager em = args.get(0);

        LOG.debug("starting transaction for [{}]", discriminator);

        EntityTransaction tx = em.getTransaction();
        tx.begin();

        return TxHolder.of(em, tx);

    }

    @Override
    public void close(Object discriminator, TxHolder resource) {
        EntityTransaction tx = resource.em().getTransaction();
        if (tx.getRollbackOnly()) {
            rollback(discriminator, tx);
        } else {
            commit(discriminator, tx);
        }
    }

    private void commit(Object discriminator, EntityTransaction tx) {
        try {
            LOG.debug("[TxManager]Committing transactions: {}",discriminator);
            tx.commit();
        } catch (Exception x) {
            throw new BoomerangCloseException("Error committing transaction", x, this);
        }
    }

    private void rollback(Object discriminator, EntityTransaction tx) {
        try {
            LOG.debug("[TxManager]Rolling back transaction for [{}]", discriminator);
            tx.rollback();
        } catch (Exception x) {
            throw new BoomerangCloseException("Error Rolling Back Transaction: ", x, this);
        }

    }

    @Override
    public void onException(Object discriminator, TxHolder resource) {
        resource.tx().setRollbackOnly();
    }

    @Override
    public String toString() {
        return "TxResource:" + txResource.currentDiscriminator().orElse(null);
    }


}
