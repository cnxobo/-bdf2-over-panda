package com.bstek.bdf2.core.orm.hibernate;

import java.util.Collection;
import java.util.Date;

import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;
import org.malagu.panda.coke.model.CokeBaseModel;
import org.malagu.panda.dorado.linq.JpaUtil;

import com.bstek.bdf2.core.business.IUser;
import com.bstek.bdf2.core.context.ContextHolder;
import com.bstek.dorado.core.Configure;

public class HibernateDao {
  
  public static final String FIXED_COMPANY_ID = "bdf2.fixedCompanyId";

  protected String getFixedCompanyId() {
    return Configure.getString(FIXED_COMPANY_ID);
  }

  protected String getModuleFixDataSourceName() {
    return null;
  }
  
  public Session getSession() {
    return JpaUtil.getEntityManager().unwrap(Session.class);
  }

  public void insertEntity(CokeBaseModel baseEntity) {
    this.insertEntity(getSession(), baseEntity, null);
    ContextHolder.getLoginUser().getCompanyId();
  }

  public void insertEntity(Session session, CokeBaseModel baseEntity, IUser user) {
    if (user == null) {
      user = ContextHolder.getLoginUser();
    }
    // baseEntity.setCreateUser(Long.valueOf(user.getUsername()));
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

  public void deleteEntity(CokeBaseModel baseEntity) {
    this.deleteEntity(getSession(), baseEntity, null);
  }

  public void deleteEntity(Session session, CokeBaseModel baseEntity, IUser user) {
    if (user == null) {
      user = ContextHolder.getLoginUser();
    }
    baseEntity.setUpdateUser(Long.valueOf(user.getUsername()));
    baseEntity.setUpdateDate(new Date());
    baseEntity.setDeleted(true);
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
