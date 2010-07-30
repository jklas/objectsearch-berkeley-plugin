package com.jklas.search.index.berkeley;

import com.jklas.search.index.MasterAndInvertedIndexWriter;
import com.jklas.search.index.IndexWriterFactory;

public class BerkeleyIndexWriterFactory implements IndexWriterFactory {

	private static BerkeleyIndexWriterFactory instance = new BerkeleyIndexWriterFactory();

	public static BerkeleyIndexWriterFactory getInstance() {
		return instance;
	}

	@Override
	public MasterAndInvertedIndexWriter getIndexWriter() {
		return new BerkeleyIndexWriter();
	}

}
