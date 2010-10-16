package com.jklas.search.index.berkeley;

public class BerkeleyGlobalPropertyEditor {

	public static String baseDir = System.getProperty("user.dir") + "\\berkeley\\";
	
	public static boolean isTransactional = false;
	
	public synchronized void setTransactional(boolean isTransactional) {
		BerkeleyGlobalPropertyEditor.isTransactional = isTransactional;
	}
	
	public synchronized boolean isTransactional() {
		return isTransactional;
	}
	
	public synchronized String getBaseDir() {
		return baseDir;
	}
	
	public synchronized void setBaseDir(String baseDir) {
		BerkeleyGlobalPropertyEditor.baseDir = baseDir;
	}
	
}
