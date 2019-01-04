package com.bstek.bdf2.core.model;

import javax.persistence.Id;

/**
 * @since 2013-1-28
 * @author Jacky.gao
 */
public class ComponentDefinition implements java.io.Serializable {
  private static final long serialVersionUID = 4314003600468849873L;

  @Id

  private String id;


  private String componentId;


  private String desc;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getComponentId() {
    return componentId;
  }

  public void setComponentId(String componentId) {
    this.componentId = componentId;
  }

  public String getDesc() {
    return desc;
  }

  public void setDesc(String desc) {
    this.desc = desc;
  }
}
