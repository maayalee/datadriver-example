package com.maayalee.dd.etls.creategooglefitnessbd;

import java.util.LinkedList;
import java.util.List;

public class TableSchemaDTO implements java.io.Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -2334774161299145978L;
  public static class TableField implements java.io.Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = -5007349491505000562L;
    public String name;
    public String type;
    public String mode;
    public Boolean isClustering;
  };
  
  public String name;
  public List<TableField> fields = new LinkedList<TableField>();
}
