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
