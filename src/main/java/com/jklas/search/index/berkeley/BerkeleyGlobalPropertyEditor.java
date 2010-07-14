package com.jklas.search.index.berkeley;

public class BerkeleyGlobalPropertyEditor {

	public static String baseDir = System.getProperty("user.dir") + "\\berkeley\\";
	
	public synchronized String getBaseDir() {
		return baseDir;
	}
	
	public synchronized void setBaseDir(String baseDir) {
		BerkeleyGlobalPropertyEditor.baseDir = baseDir;
	}
	
}
