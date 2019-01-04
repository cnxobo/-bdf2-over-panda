package com.bstek.bdf2.core.orm.hibernate;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.CriteriaSpecification;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.MatchMode;
import org.hibernate.criterion.Projection;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Property;
import org.hibernate.criterion.Restrictions;
import org.hibernate.internal.CriteriaImpl;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import com.bstek.bdf2.core.orm.AbstractDao;
import com.bstek.dorado.data.provider.And;
import com.bstek.dorado.data.provider.Criteria;
import com.bstek.dorado.data.provider.Junction;
import com.bstek.dorado.data.provider.Or;
import com.bstek.dorado.data.provider.Page;
import com.bstek.dorado.data.provider.filter.SingleValueFilterCriterion;

/**
 * @since 2013-1-17
 * @author Jacky.gao
 */
@SuppressWarnings("deprecation")
public abstract class HibernateDao extends AbstractDao implements ApplicationContextAware {
  private SessionFactory sessionFactory;
  private ApplicationContext applicationContext;


  public Session getSession() {
    return getSessionFactory().openSession();
  }

  public Session getSession(String dataSourceName) {
    return getSession();
  }


  public SessionFactory getSessionFactory() {
    return sessionFactory;
  }


  public SessionFactory getSessionFactory(String dataSourceName) {
    return getSessionFactory();
  }

  public Collection<?> query(DetachedCriteria detachedCriteria) {
    return this.query(detachedCriteria, null);
  }

  public Collection<?> query(DetachedCriteria detachedCriteria, String dataSourceName) {
    Session session = this.getSessionFactory(dataSourceName).openSession();
    try {
      return detachedCriteria.getExecutableCriteria(session).list();
    } finally {
      session.flush();
      session.close();
    }
  }

  public Collection<?> query(DetachedCriteria detachedCriteria, int pageIndex, int pageSize) {
    return this.query(detachedCriteria, pageIndex, pageSize, null);
  }

  public Collection<?> query(DetachedCriteria detachedCriteria, int pageIndex, int pageSize,
      String dataSourceName) {
    Session session = this.getSessionFactory(dataSourceName).openSession();
    try {
      return detachedCriteria.getExecutableCriteria(session)
          .setFirstResult((pageIndex - 1) * pageSize).setMaxResults(pageSize).list();
    } finally {
      session.flush();
      session.close();
    }
  }

  public int queryCount(DetachedCriteria detachedCriteria) {
    return this.queryCount(detachedCriteria, null);
  }

  @SuppressWarnings("rawtypes")
  public int queryCount(DetachedCriteria detachedCriteria, String dataSourceName) {
    Session session = this.getSessionFactory(dataSourceName).openSession();
    try {
      org.hibernate.Criteria criteria = detachedCriteria.getExecutableCriteria(session);
      try {
        Field field = CriteriaImpl.class.getDeclaredField("orderEntries");
        field.setAccessible(true);
        field.set(criteria, new ArrayList());
      } catch (Exception e) {
        e.printStackTrace();
      }
      int totalCount = 0;
      Object totalObject = criteria.setProjection(Projections.rowCount()).uniqueResult();
      if (totalObject instanceof Integer) {
        totalCount = (Integer) totalObject;
      } else if (totalObject instanceof Long) {
        totalCount = ((Long) totalObject).intValue();
      } else if (totalObject != null) {
        throw new RuntimeException(
            "Can not convert [" + totalObject + "] to a int value when query entity total count!");
      }
      return totalCount;
    } finally {
      session.flush();
      session.close();
    }
  }

  public void pagingQuery(Page<?> page, DetachedCriteria detachedCriteria) {
    this.pagingQuery(page, detachedCriteria, null);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void pagingQuery(Page<?> page, DetachedCriteria detachedCriteria, String dataSourceName) {
    Session session = this.getSessionFactory(dataSourceName).openSession();
    try {
      org.hibernate.Criteria criteria = detachedCriteria.getExecutableCriteria(session);
      CriteriaImpl criteriaImpl = (CriteriaImpl) criteria;
      List orderEntrys = new ArrayList();
      try {
        Field field = CriteriaImpl.class.getDeclaredField("orderEntries");
        field.setAccessible(true);
        orderEntrys = (List) field.get(criteriaImpl);
        field.set(criteria, new ArrayList());
      } catch (Exception e) {
        e.printStackTrace();
      }
      Projection projection = criteriaImpl.getProjection();
      int totalCount = 0;
      Object totalObject = criteria.setProjection(Projections.rowCount()).uniqueResult();
      if (totalObject instanceof Integer) {
        totalCount = (Integer) totalObject;
      } else if (totalObject instanceof Long) {
        totalCount = ((Long) totalObject).intValue();
      } else if (totalObject != null) {
        throw new RuntimeException(
            "Can not convert [" + totalObject + "] to a int value when query entity total count!");
      }
      page.setEntityCount(totalCount);

      try {
        Field field = CriteriaImpl.class.getDeclaredField("orderEntries");
        field.setAccessible(true);
        for (int i = 0; i < orderEntrys.size(); i++) {
          List innerOrderEntries = (List) field.get(criteriaImpl);
          innerOrderEntries.add(orderEntrys.get(i));
        }
      } catch (Exception ex) {
        ex.printStackTrace();
      }

      criteria.setProjection(projection);
      if (projection == null) {
        criteria.setResultTransformer(CriteriaSpecification.ROOT_ENTITY);
      }
      int start = (page.getPageNo() - 1) * page.getPageSize();
      page.setEntities(detachedCriteria.getExecutableCriteria(session).setFirstResult(start)
          .setMaxResults(page.getPageSize()).list());
    } finally {
      session.flush();
      session.close();
    }
  }

  /**
   * 基于Hibernate的分页查询
   * 
   * @param page 包含pageNo与pageSize值的dorado7 page对象
   * @param queryString 查询目标结果用的HQL
   * @param dataSourceName要采用的目标数据源名称
   * @throws Exception 可能抛出的异常
   */
  public void pagingQuery(Page<?> page, String queryString, String countQueryString,
      String dataSourceName) throws Exception {
    this.pagingQuery(page, queryString, countQueryString, null, null, dataSourceName);
  }

  /**
   * 基于Hibernate的分页查询
   * 
   * @param page 包含pageNo与pageSize值的dorado7 page对象
   * @param queryString 查询目标结果用的HQL
   * @throws Exception 可能抛出的异常
   */
  public void pagingQuery(Page<?> page, String queryString, String countQueryString)
      throws Exception {
    this.pagingQuery(page, queryString, countQueryString, null, null, null);
  }

  public List<?> query(String queryString, Map<String, Object> parametersMap, int pageIndex,
      int pageSize) {
    return this.query(queryString, parametersMap, pageIndex, pageSize, null);
  }

  @SuppressWarnings({"rawtypes"})
  public List<?> query(String queryString, Map<String, Object> parametersMap, int pageIndex,
      int pageSize, String dataSourceName) {
    Session session = this.getSessionFactory(dataSourceName).openSession();
    try {
      Query query = session.createQuery(queryString);
      return query.setFirstResult((pageIndex - 1) * pageSize).setMaxResults(pageSize).list();
    } finally {
      session.flush();
      session.close();
    }
  }

  /**
   * 基于Hibernate的分页查询
   * 
   * @param page 包含pageNo与pageSize值的dorado7 page对象
   * @param queryString 查询目标结果用的HQL
   * @param parameters 查询中可能包含的查询条件值,按位置顺序拼装参数方式
   * @param parametersMap 查询中可能包含的查询条件值,按参数名拼装参数方式，与参数parameters只能二选一
   * @param dataSourceName要采用的目标数据源名称
   * @throws Exception 可能抛出的异常
   */
  @SuppressWarnings({"rawtypes"})
  protected void pagingQuery(final Page<?> page, final String queryString,
      final String countQueryString, final Object[] parameters,
      final Map<String, Object> parametersMap, String dataSourceName) throws Exception {
    this.doInHibernateSession(dataSourceName, new ISessionCallback<Object>() {
      @SuppressWarnings("unchecked")
      public Object doInSession(Session session) {
        String queryHql = queryString.trim();
        Query query = session.createQuery(queryHql);
        int safePageSize = page.getPageSize() < 1 ? 65535 : page.getPageSize();
        int start = (page.getPageNo() - 1) * safePageSize;
        if (parameters != null) {
          setQueryParameters(query, parameters);
        } else if (parametersMap != null) {
          setQueryParameters(query, parametersMap);
        }
        query.setMaxResults(safePageSize).setFirstResult(start);
        page.setEntities(query.list());
        Query countQuery = session.createQuery(countQueryString);
        if (parameters != null) {
          setQueryParameters(countQuery, parameters);
        } else if (parametersMap != null) {
          setQueryParameters(countQuery, parametersMap);
        }
        int count = 0;
        Object countObj = countQuery.uniqueResult();
        if (countObj instanceof Long) {
          count = ((Long) countObj).intValue();
        } else if (countObj instanceof Integer) {
          count = ((Integer) countObj).intValue();
        }
        page.setEntityCount(count);
        return null;
      }
    });
  }

  public void pagingQuery(Page<?> page, String queryString, String countQueryString,
      Map<String, Object> parametersMap, String dataSourceName) throws Exception {
    this.pagingQuery(page, queryString, countQueryString, null, parametersMap, dataSourceName);
  }

  public void pagingQuery(Page<?> page, String queryString, String countQueryString,
      Map<String, Object> parametersMap) throws Exception {
    this.pagingQuery(page, queryString, countQueryString, null, parametersMap, null);
  }

  @SuppressWarnings({"rawtypes"})
  public void setQueryParameters(Query query, Object[] parameters) {
    if (parameters == null)
      return;
    for (int i = 0; i < parameters.length; i++) {
      query.setParameter(i, parameters[i]);
    }
  }

  @SuppressWarnings({"rawtypes"})
  public void setQueryParameters(Query query, Map<String, Object> parameters) {
    if (parameters == null)
      return;
    for (String name : parameters.keySet()) {
      Object obj = parameters.get(name);
      if (obj instanceof Collection) {
        query.setParameterList(name, (Collection<?>) obj);
      } else if (obj instanceof Object[]) {
        query.setParameterList(name, (Object[]) obj);
      } else {
        query.setParameter(name, obj);
      }
    }
  }

  public <T> List<T> query(String hql, String dataSourceName) {
    return this.query(hql, null, null, dataSourceName);
  }

  public <T> List<T> query(String hql) {
    return this.query(hql, null, null, null);
  }

  public <T> List<T> query(String hql, Map<String, Object> parameterMap) {
    return this.query(hql, null, parameterMap, null);
  }

  public <T> List<T> query(String hql, Map<String, Object> parameterMap, String dataSourceName) {
    return this.query(hql, null, parameterMap, dataSourceName);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private <T> List<T> query(String hql, Object[] parameters, Map<String, Object> parameterMap,
      String dataSourceName) {
    SessionFactory factory = this.getSessionFactory(dataSourceName);
    Session session = factory.openSession();
    List<T> result = null;
    try {
      Query query = session.createQuery(hql);
      if (parameters != null) {
        this.setQueryParameters(query, parameters);
      } else if (parameterMap != null) {
        this.setQueryParameters(query, parameterMap);
      }
      result = query.list();
    } finally {
      session.close();
    }
    return result;
  }

  public int queryForInt(String hql) {
    return this.queryForInt(hql, null, null, null);
  }

  public int queryForInt(String hql, Map<String, Object> parameterMap) {
    return this.queryForInt(hql, null, parameterMap, null);
  }

  public int queryForInt(String hql, Map<String, Object> parameterMap, String dataSourceName) {
    return this.queryForInt(hql, null, parameterMap, dataSourceName);
  }

  @SuppressWarnings({"rawtypes"})
  private int queryForInt(String hql, Object[] parameters, Map<String, Object> parameterMap,
      String dataSourceName) {
    SessionFactory factory = this.getSessionFactory(dataSourceName);
    Session session = factory.openSession();
    int count = 0;
    try {
      Query countQuery = session.createQuery(hql);
      if (parameters != null) {
        setQueryParameters(countQuery, parameters);
      } else if (parameterMap != null) {
        setQueryParameters(countQuery, parameterMap);
      }
      Object countObj = countQuery.uniqueResult();
      if (countObj instanceof Long) {
        count = ((Long) countObj).intValue();
      } else if (countObj instanceof Integer) {
        count = ((Integer) countObj).intValue();
      }
    } finally {
      session.close();
    }
    return count;
  }

  /**
   * 采用默认数据源执行一个实现了IHibernateJob接口的业务操作类，可直接使用其中的的session对象，<br>
   * 使用完毕之后由该方法将其close，业务方法不需要将session做close操作
   * 
   * @param callback 一个实现了IHibernateJob接口的业务操作类
   * @return 返回操作中需要返回的结果对象
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public <T> T doInHibernateSession(ISessionCallback<T> callback) {
    String currentDatasourceName = this.getDataSourceName(null);
    return (T) this.doInHibernateSession(currentDatasourceName, callback);
  }

  /**
   * 执行一个实现了IHibernateJob接口的业务操作类，可直接使用其中的的session对象，<br>
   * 使用完毕之后由该方法将其close，业务方法不需要将session做close操作
   * 
   * @param job 一个实现了IHibernateJob接口的业务操作类
   * @param dataSourceName要采用的数据源名称
   * @return 返回操作中需要返回的结果对象
   * @throws Exception
   */
  public Object doInHibernateSession(String dataSourceName, ISessionCallback<?> job) {
    Session session = this.getSessionFactory(dataSourceName).openSession();
    Object result = null;
    try {
      result = job.doInSession(session);
    } finally {
      session.flush();
      session.close();
    }
    return result;
  }

  public DetachedCriteria buildDetachedCriteria(Criteria criteria, Class<?> entityClass) {
    return buildDetachedCriteria(criteria, entityClass, null);
  }

  public DetachedCriteria buildDetachedCriteria(Criteria criteria, Class<?> entityClass,
      String alias) {
    DetachedCriteria dc = null;
    if (StringUtils.isEmpty(alias)) {
      dc = DetachedCriteria.forClass(entityClass);
    } else {
      dc = DetachedCriteria.forClass(entityClass, alias);
    }
    if (criteria != null && criteria.getCriterions().size() > 0) {
      buildCriterions(criteria.getCriterions(), dc);
    }
    return dc;
  }

  private void buildCriterions(Collection<com.bstek.dorado.data.provider.Criterion> criterions,
      DetachedCriteria dc) {
    for (com.bstek.dorado.data.provider.Criterion c : criterions) {
      if (c instanceof SingleValueFilterCriterion) {
        SingleValueFilterCriterion fc = (SingleValueFilterCriterion) c;
        dc.add(this.buildCriterion(fc));
      }
      if (c instanceof Junction) {
        Junction jun = (Junction) c;
        org.hibernate.criterion.Junction junction = null;
        if (jun instanceof Or) {
          junction = Restrictions.disjunction();
        } else if (jun instanceof And) {
          junction = Restrictions.conjunction();
        }
        Collection<com.bstek.dorado.data.provider.Criterion> subCriterions = jun.getCriterions();
        if (subCriterions != null) {
          buildCriterions(subCriterions, junction);
        }
        dc.add(junction);
      }
    }
  }

  private void buildCriterions(Collection<com.bstek.dorado.data.provider.Criterion> criterions,
      org.hibernate.criterion.Junction dc) {
    for (com.bstek.dorado.data.provider.Criterion c : criterions) {
      if (c instanceof SingleValueFilterCriterion) {
        SingleValueFilterCriterion fc = (SingleValueFilterCriterion) c;
        dc.add(this.buildCriterion(fc));
      }
      if (c instanceof Junction) {
        Junction jun = (Junction) c;
        org.hibernate.criterion.Junction junction = null;
        if (jun instanceof Or) {
          junction = Restrictions.disjunction();
        } else if (jun instanceof And) {
          junction = Restrictions.conjunction();
        }
        Collection<com.bstek.dorado.data.provider.Criterion> subCriterions = jun.getCriterions();
        if (subCriterions != null) {
          buildCriterions(subCriterions, dc);
        }
        dc.add(junction);
      }
    }
  }

  private Criterion buildCriterion(SingleValueFilterCriterion fc) {
    Criterion result = null;
    String operator = buildOperator(fc.getFilterOperator());
    String propertyName = fc.getProperty();
    Property p = Property.forName(propertyName);
    if (operator.equals("like")) {
      result = p.like("%" + fc.getValue() + "%");
    } else if (operator.startsWith("*")) {
      result = p.like((String) fc.getValue(), MatchMode.END);
    } else if (operator.endsWith("*")) {
      result = p.like((String) fc.getValue(), MatchMode.START);
    } else if (operator.equals(">")) {
      result = p.gt(fc.getValue());
    } else if (operator.equals("<")) {
      result = p.lt(fc.getValue());
    } else if (operator.equals(">=")) {
      result = p.ge(fc.getValue());
    } else if (operator.equals("<=")) {
      result = p.le(fc.getValue());
    } else if (operator.equals("=")) {
      result = p.eq(fc.getValue());
    } else if (operator.equals("<>")) {
      result = p.ne(fc.getValue());
    } else {
      throw new IllegalArgumentException("Query operator[" + operator + "] is invalide");
    }
    return result;
  }

  public ApplicationContext getApplicationContext() {
    return applicationContext;
  }

  public void setApplicationContext(ApplicationContext applicationContext)
      throws BeansException {
    this.applicationContext = applicationContext;

  }
}
