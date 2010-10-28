package com.jklas.search.index.berkeley;

import com.jklas.search.index.IndexId;
import com.jklas.search.index.IndexReaderFactory;
import com.jklas.search.index.MasterAndInvertedIndexReader;

public class BerkeleyIndexReaderFactory implements IndexReaderFactory {

	private static BerkeleyIndexReaderFactory instance = new BerkeleyIndexReaderFactory();

	public static BerkeleyIndexReaderFactory getInstance() {
		return instance;
	}

	private BerkeleyIndexReaderFactory() {
	}

	@Override
	public MasterAndInvertedIndexReader getIndexReader() {
		return new BerkeleyIndexReader(IndexId.getDefaultIndexId());
	}

	@Override
	public MasterAndInvertedIndexReader getIndexReader(IndexId indexId) {
		return new BerkeleyIndexReader(indexId);
	}

}
