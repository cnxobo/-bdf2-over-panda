package com.bstek.bdf2.core.orm.hibernate;

import java.util.Collection;
import java.util.Date;

import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;
import org.malagu.panda.coke.model.CokeBaseModel;
import org.malagu.panda.dorado.linq.JpaUtil;

import com.bstek.bdf2.core.business.IUser;
import com.bstek.bdf2.core.context.ContextHolder;

public class HibernateDao {
  
  public Session getSession() {
    return JpaUtil.getEntityManager().unwrap(Session.class);
  }

  public  void insertEntity(CokeBaseModel baseEntity) {
    this.insertEntity(getSession(), baseEntity, null);
  }
  
  public void insertEntity(Session session, CokeBaseModel baseEntity, IUser user) {
    if (user == null) {
      user = ContextHolder.getLoginUser();
    }
//     baseEntity.setCreateUser(Long.valueOf(user.getUsername()));
    baseEntity.setCreateDate(new Date());
    baseEntity.setDeleted(false);
    session.save(baseEntity);
  }

  public void updateEntity(CokeBaseModel baseEntity) {
    this.updateEntity(getSession(), baseEntity, null);
  }
  
  public void updateEntity(Session session, CokeBaseModel baseEntity, IUser user) {
    if (user == null) {
      user = ContextHolder.getLoginUser();
    }
    baseEntity.setUpdateUser(Long.valueOf(user.getUsername()));
    baseEntity.setUpdateDate(new Date());
    baseEntity.setDeleted(false);
    session.saveOrUpdate(baseEntity);
  }
  
  public Collection<?> query(DetachedCriteria detachedCriteria) {
    Session session = getSession();
    try {
      return detachedCriteria.getExecutableCriteria(session).list();
    } finally {
      session.flush();
      session.close();
    }
  }
  
}
