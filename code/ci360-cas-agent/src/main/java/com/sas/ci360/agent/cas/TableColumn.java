package com.sas.ci360.agent.cas;

import com.sas.cas.actions.table.Addtablevariable.TYPE;

public class TableColumn {
	private String name;
	private TYPE type;	
	
	public TableColumn() {		
	}
	
	public TableColumn(String name, TYPE type) {
		this.name = name;
		this.type = type;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public TYPE getType() {
		return type;
	}
	public void setType(TYPE type) {
		this.type = type;
	}
	
}
