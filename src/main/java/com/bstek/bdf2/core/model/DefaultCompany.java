package com.bstek.bdf2.core.model;

/**
 * @since 2013-1-27
 * @author Jacky.gao
 */
public class DefaultCompany implements java.io.Serializable {
  private static final long serialVersionUID = -6326282268106128108L;

  private String id;

  private String name;

  private String desc;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDesc() {
    return desc;
  }

  public void setDesc(String desc) {
    this.desc = desc;
  }
}
