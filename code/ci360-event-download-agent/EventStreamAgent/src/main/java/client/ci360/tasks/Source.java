/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package client.ci360.tasks;

/**
 *
 * @author sas
 */
public class Source {
	public static final Source UDM_BY_VERSION = new Source("UDM_BY_VERSION");
	public static final Source UDM_BY_ID = new Source("UDM_BY_ID");
	public static final Source TASK_API = new Source("TASK_API");
	public static final Source LIST_API = new Source("LIST_API");

	private String text;

	private Source(String text) {
		this.text = text;
	}

	public String toString() {
		return text;
	}
}
