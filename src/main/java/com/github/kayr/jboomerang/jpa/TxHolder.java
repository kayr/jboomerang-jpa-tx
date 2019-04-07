package com.github.kayr.jboomerang.jpa;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;

public class TxHolder {
    private EntityManager em;
    private EntityTransaction tx;

    static TxHolder of(EntityManager em, EntityTransaction tx) {
        TxHolder txHolder = new TxHolder();
        txHolder.em = em;
        txHolder.tx = tx;
        return txHolder;
    }

    public EntityManager em() {
        return em;
    }

    public EntityTransaction tx() {
        return tx;
    }


}
