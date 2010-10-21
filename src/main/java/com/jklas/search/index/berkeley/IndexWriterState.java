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
package com.jklas.search.index.berkeley.utils;

import java.io.Serializable;
import java.util.Map.Entry;

import com.jklas.search.index.IndexId;
import com.jklas.search.index.ObjectKey;
import com.jklas.search.index.PostingList;
import com.jklas.search.index.PostingMetadata;
import com.jklas.search.index.Term;
import com.jklas.search.index.berkeley.BerkeleyGlobalPropertyEditor;
import com.jklas.search.index.berkeley.BerkeleyIndex;

public class WordInspector {

	public static void main(String[] args) {
		
		new WordInspector().dumpPosting("/home/julian/tesis-workspace/kstore/berkeley/","MP3", null);
		
	}

	private void dumpPosting(String baseDir, String word, String indexName) {
		new BerkeleyGlobalPropertyEditor().setBaseDir(baseDir);
		
		IndexId indexId;
		if(indexName==null) indexId = IndexId.getDefaultIndexId();
		else indexId = new IndexId(indexName);
			
		PostingList postingList = BerkeleyIndex.getIndex(indexId).getPostingList(new Term(word));
		
		if(postingList == null) {
			System.out.println("No results for word '"+word+"'");
		} else {
			int termCount = postingList.getTermCount();
			System.out.println("Posting List for term '"+word+"' - Term Count: " + termCount );
			
			for (Entry<ObjectKey, PostingMetadata> entry : postingList) {
				Class<?> clazz = entry.getKey().getClazz();
				Serializable id = entry.getKey().getId();
				
				System.out.println("----> "+clazz.getCanonicalName());
				System.out.println("  '-> "+id);
				System.out.println("\n");
			}
		}
	}
	
}
