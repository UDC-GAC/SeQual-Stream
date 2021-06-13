package com.oscar.castellanos.sequal.sequalmodel.stream.utils;

public class ControlFinishReader {
	
	/** when this flag is set to false, it means that read from input file has not ended */
	private boolean flag;
	private static ControlFinishReader control;
	
	synchronized public static ControlFinishReader getControl() {
		if (control == null) {
			control = new ControlFinishReader();
		}
		return control;
	}
	
	private ControlFinishReader() {
		this.flag = false;
	}
	
	public boolean isFlag() {
		return flag;
	}

	public void setFlag(boolean flag) {
		this.flag = flag;
	}
	
}
