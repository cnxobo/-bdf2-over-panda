package com.bstek.bdf2.core.orm.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Resource;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.bstek.bdf2.core.orm.AbstractDao;
import com.bstek.bdf2.core.orm.DataSourceRepository;

import org.apache.commons.lang.StringUtils;
import org.malagu.panda.coke.datasource.service.DataSourceInfoService;
import org.malagu.panda.dbconsole.jdbc.dialect.IDialect;
import com.bstek.dorado.data.provider.Page;
import com.itextpdf.text.pdf.PdfStructTreeController.returnType;

/**
 * @since 2013-1-17
 * @author Jacky.gao
 */
public abstract class JdbcDao extends AbstractDao implements ApplicationContextAware {
  private ApplicationContext applicationContext;
  private DataSourceRepository dataSourceRepository;
  private Collection<IDialect> dialects;
  
  private Map<String, JdbcTemplate> jdbcTemplateMap = new ConcurrentHashMap<String, JdbcTemplate>();
  private Map<String, NamedParameterJdbcTemplate> nameJdbcTemplateMap = new ConcurrentHashMap<String, NamedParameterJdbcTemplate>();
  
  protected void pagingQuery(Page<?> page, String sql, String dataSourceName, RowMapper<?> mapper,
      Map<String, Object> parameters) {
    this.pagingQuery(page, sql, dataSourceName, parameters, mapper);
  }

  protected void pagingQuery(Page<?> page, String sql, String dataSourceName,
      Map<String, Object> parameters) {
    this.pagingQuery(page, sql, dataSourceName, parameters, null);
  }

  protected void pagingQuery(Page<?> page, String sql, Map<String, Object> parameters) {
    this.pagingQuery(page, sql, null, parameters, null);
  }

  protected void pagingQuery(Page<?> page, String sql, RowMapper<?> mapper,
      Map<String, Object> parameters) {
    this.pagingQuery(page, sql, null, parameters, mapper);
  }

  protected void pagingQuery(Page<?> page, String sql, String dataSourceName, RowMapper<?> mapper) {
    pagingQuery(page, sql, null, dataSourceName, mapper);
  }

  protected void pagingQuery(Page<?> page, String sql, RowMapper<?> mapper) {
    String currentDatasourceName = this.getDataSourceName(null);
    pagingQuery(page, sql, null, currentDatasourceName, mapper);
  }

  protected void pagingQuery(Page<?> page, String sql, Object parameters[], RowMapper<?> mapper) {
    String currentDatasourceName = this.getDataSourceName(null);
    pagingQuery(page, sql, parameters, currentDatasourceName, mapper);
  }


  protected void pagingQuery(Page<?> page, String sql, String dataSourceName) {
    pagingQuery(page, sql, null, dataSourceName, null);
  }

  protected void pagingQuery(Page<?> page, String sql) {
    String currentDatasourceName = this.getDataSourceName(null);
    pagingQuery(page, sql, null, currentDatasourceName, null);
  }

  protected void pagingQuery(Page<?> page, String sql, Object parameters[]) {
    String currentDatasourceName = this.getDataSourceName(null);
    pagingQuery(page, sql, parameters, currentDatasourceName, null);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected void pagingQuery(Page<?> page, String sql, Object parameters[], String dataSourceName,
      RowMapper<?> mapper) {
    JdbcTemplate currentJdbcTemplate = this.getJdbcTemplate(dataSourceName);
    IDialect dialect = getDialect(currentJdbcTemplate);
    if (dialect == null) {
      throw new RuntimeException("无法找到与当前数据连接匹配的数据库方言类");
    }
    String querySql = dialect.getPaginationSql(sql, page.getPageNo(), page.getPageSize());
    String countSql = "select count(*) from (" + sql + ") sub_table_alias_";
    if (parameters != null) {
      if (mapper == null) {
        page.setEntities((List) currentJdbcTemplate.queryForList(querySql, parameters));
      } else {
        page.setEntities((List) currentJdbcTemplate.query(querySql, parameters, mapper));
      }
      page.setEntityCount(currentJdbcTemplate.queryForObject(countSql, parameters, Integer.class));
    } else {
      if (mapper == null) {
        page.setEntities((List) currentJdbcTemplate.queryForList(querySql));
      } else {
        page.setEntities((List) currentJdbcTemplate.query(querySql, mapper));
      }
      page.setEntityCount(currentJdbcTemplate.queryForObject(countSql, Integer.class));
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected void pagingQuery(Page<?> page, String sql, String dataSourceName,
      Map<String, Object> parameters, RowMapper<?> mapper) {
    JdbcTemplate currentJdbcTemplate = this.getJdbcTemplate(dataSourceName);
    NamedParameterJdbcTemplate currentNameJdbcTemplate = this.getNamedParameterJdbcTemplate(dataSourceName);
    String querySql = this.getDialect(currentJdbcTemplate).getPaginationSql(sql,
        page.getPageNo(), page.getPageSize());
    String countSql = "select count(*) from (" + sql + ") sub_table_alias_";
    if (parameters != null) {
      if (mapper == null) {
        page.setEntities((List) currentNameJdbcTemplate.queryForList(querySql, parameters));
      } else {
        page.setEntities((List) currentNameJdbcTemplate.query(querySql, parameters, mapper));
      }
      page.setEntityCount(
          currentNameJdbcTemplate.queryForObject(countSql, parameters, Integer.class));
    } else {
      if (mapper == null) {
        page.setEntities((List) currentJdbcTemplate.queryForList(querySql));
      } else {
        page.setEntities((List) currentJdbcTemplate.query(querySql, mapper));
      }
      page.setEntityCount(currentJdbcTemplate.queryForObject(countSql, Integer.class));
    }
  }

  public JdbcTemplate getJdbcTemplate(String dataSourceName) {
    if (StringUtils.isNotEmpty(dataSourceName)) {
      JdbcTemplate currentJdbcTemplate = jdbcTemplateMap.get(dataSourceName);
      if (currentJdbcTemplate == null) {
        JdbcDaoSupport jdbcDaoSupport = new JdbcDaoSupport() {};
        jdbcDaoSupport.setDataSource(dataSourceInfoService.getDataSource(dataSourceName));
        JdbcTemplate newJdbcTemplate = jdbcDaoSupport.getJdbcTemplate();
        jdbcTemplateMap.put(dataSourceName, newJdbcTemplate);
        return newJdbcTemplate;
      } else {
        return currentJdbcTemplate;
      }
    } 
    return this.jdbcTemplate;
  }
  
  public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(String dataSourceName) {
    if (StringUtils.isNotEmpty(dataSourceName)) {
      NamedParameterJdbcTemplate currentNameJdbcTemplate = nameJdbcTemplateMap.get(dataSourceName);
      if (currentNameJdbcTemplate == null) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate(dataSourceName);
        NamedParameterJdbcTemplate newNameJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        nameJdbcTemplateMap.put(dataSourceName, newNameJdbcTemplate);
        return newNameJdbcTemplate;
      } else {
        return currentNameJdbcTemplate;
      }
    }
    return namedParameterJdbcTemplate;
  }

  protected IDialect getDialect(JdbcTemplate jdbcTemplate) {
    return jdbcTemplate.execute(new ConnectionCallback<IDialect>() {
      public IDialect doInConnection(Connection connection) throws SQLException,
          DataAccessException {
        IDialect result = null;
        for (IDialect dialect : dialects) {
          if (dialect.support(connection)) {
            result = dialect;
            break;
          }
        }
        return result;
      }
    });
  }

  protected ApplicationContext getApplicationContext() {
    return applicationContext;
  }

  public void setApplicationContext(ApplicationContext applicationContext)
      throws BeansException {
    this.applicationContext = applicationContext;
    Collection<DataSourceRepository> dataSourceRepositoryCollection =
        applicationContext.getBeansOfType(DataSourceRepository.class).values();
    if (dataSourceRepositoryCollection.size() > 0) {
      this.dataSourceRepository = dataSourceRepositoryCollection.iterator().next();
    } else if (applicationContext.getParent() != null) {
      dataSourceRepositoryCollection =
          applicationContext.getParent().getBeansOfType(DataSourceRepository.class).values();
      if (dataSourceRepositoryCollection.size() > 0) {
        this.dataSourceRepository = dataSourceRepositoryCollection.iterator().next();
      }
    }
    this.dialects = applicationContext.getBeansOfType(IDialect.class).values();
  }

  @Resource(name = DataSourceInfoService.BEAN_ID)
  private DataSourceInfoService dataSourceInfoService;
  
  @Autowired
  private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate() {
    return namedParameterJdbcTemplate;
  }

  public void setNamedParameterJdbcTemplate(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  public JdbcTemplate getJdbcTemplate() {
    return jdbcTemplate;
  }

  public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }
  
}
