package com.github.kayr.jboomerang.jpa;

import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;

import javax.persistence.EntityManager;
import java.sql.Connection;

public class EntityMangerUtil {


    public static Connection extractConnection(EntityManager em) {
        SessionImpl unwrap = (SessionImpl) em.unwrap(Session.class);
        return unwrap.connection();
    }

}
