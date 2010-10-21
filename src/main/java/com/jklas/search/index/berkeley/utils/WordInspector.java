/**
 * Object Search Framework
 *
 * Copyright (C) 2010 Julian Klas
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
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
