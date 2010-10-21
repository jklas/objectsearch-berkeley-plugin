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

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.jklas.search.index.IndexId;
import com.jklas.search.index.MasterAndInvertedIndex;
import com.jklas.search.index.MasterRegistryEntry;
import com.jklas.search.index.ObjectKey;
import com.jklas.search.index.PostingList;
import com.jklas.search.index.PostingMetadata;
import com.jklas.search.index.Term;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BerkeleyIndex implements MasterAndInvertedIndex {

	private static StoredClassCatalog javaCatalog;

	private static Environment env;

	private static final String CLASS_CATALOG = "java_class_catalog";

	private static final String AVAILABLE_INDEXES_DB = "AVAILABLE_INDEXES_DB";

	private static final String THIS_INDEX_PREFIX = "IDX";

	private static boolean globalInit = false;

	private static Database availableIndexesDb;

	private static Map<IndexId, BerkeleyIndex> availableOpenIndexes ;
	
	private static StoredMap<IndexId, BerkeleyIndexMetadata> availableIndexes;

	private boolean thisIndexInit = false;

	private IndexId indexId;
	
	private Database masterRegistryDb;

	private Database invertedIndexDb;

	private StoredMap<ObjectKey,MasterRegistryEntry> masterRegistry;

	private StoredMap<Term, PostingList> invertedIndex;

	public BerkeleyIndex(IndexId indexId) {
		checkGlobalInit();
		indexInit(indexId);
		this.indexId = indexId;
	}

	private void indexInit(IndexId indexId) {
		if(availableOpenIndexes.containsKey(indexId)) {
			BerkeleyIndex alreadyOpen = availableOpenIndexes.get(indexId);
			this.invertedIndex = alreadyOpen.invertedIndex;
			this.masterRegistry = alreadyOpen.masterRegistry;
			this.masterRegistryDb = alreadyOpen.masterRegistryDb;
			this.invertedIndexDb = alreadyOpen.invertedIndexDb;
			return;
		} else {
			if(!thisIndexInit) {			
				DatabaseConfig dbConfig = new DatabaseConfig();
				dbConfig.setTransactional(new BerkeleyGlobalPropertyEditor().isTransactional());
				dbConfig.setAllowCreate(true);
				
				invertedIndexDb = env.openDatabase(null, THIS_INDEX_PREFIX + ".INV." + indexId.getIndexName(), dbConfig);		        
				masterRegistryDb = env.openDatabase(null, THIS_INDEX_PREFIX + ".MST." + indexId.getIndexName(), dbConfig);
				
				
				EntryBinding<ObjectKey> masterKeyBinding = new SerialBinding<ObjectKey>(javaCatalog, ObjectKey.class);
				EntryBinding<MasterRegistryEntry> masterValueBinding = new SerialBinding<MasterRegistryEntry>(javaCatalog, MasterRegistryEntry.class);
				masterRegistry = new StoredMap<ObjectKey,MasterRegistryEntry>(masterRegistryDb,masterKeyBinding, masterValueBinding, true);
				
				EntryBinding<Term> invertedKeyBinding = new SerialBinding<Term>(javaCatalog, Term.class);
				EntryBinding<PostingList> invertedValueBinding = new SerialBinding<PostingList>(javaCatalog, PostingList.class);
				invertedIndex = new StoredMap<Term, PostingList>(invertedIndexDb,invertedKeyBinding, invertedValueBinding, true);
				
				availableIndexes.put(indexId, new BerkeleyIndexMetadata());
				availableOpenIndexes.put(indexId, this);
			}			
		}
		
		thisIndexInit = true;
	}

	private synchronized static void checkGlobalInit() {
		if(!globalInit) {
			BerkeleyGlobalPropertyEditor berkeleyGlobalPropertyEditor = new BerkeleyGlobalPropertyEditor();
			
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setTransactional(true);
			envConfig.setAllowCreate(true);
			
			env = new Environment(new File(berkeleyGlobalPropertyEditor.getBaseDir()), envConfig);

			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setTransactional(true);
			dbConfig.setAllowCreate(true);

			Database catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);

			javaCatalog = new StoredClassCatalog(catalogDb);
			EntryBinding<IndexId> aiKeyBinding = new SerialBinding<IndexId>(javaCatalog, IndexId.class);
			EntryBinding<BerkeleyIndexMetadata> aiValueBinding = new SerialBinding<BerkeleyIndexMetadata>(javaCatalog, BerkeleyIndexMetadata.class);

			availableIndexesDb = env.openDatabase(null, AVAILABLE_INDEXES_DB, dbConfig);
			availableIndexes = new StoredMap<IndexId, BerkeleyIndexMetadata>(availableIndexesDb,aiKeyBinding, aiValueBinding, true);
			availableOpenIndexes = new ConcurrentHashMap<IndexId, BerkeleyIndex>();
			globalInit = true;
		}
	}

	@Override
	public void consistentRemove(ObjectKey key, List<Term> termList) {
		masterRegistry.remove(key);

		for (Term term : termList) {
			PostingList postingList = invertedIndex.get(term);
			postingList.remove(key);
			invertedIndex.put(term, postingList);
		}

		removeFromMasterRegistry(key);
	}

	@Override
	public void consistentRemove(ObjectKey key) {
		MasterRegistryEntry masterRegistryEntry = masterRegistry.get(key);

		if(masterRegistryEntry == null) return;

		Set<Term> termSet = masterRegistryEntry.getTerms();

		if(termSet == null) return;

		for (Term term : termSet) {
			removePosting(term, key);
		}

		removeFromMasterRegistry(key);
	}

	@Override
	public int getObjectCount() {
		return masterRegistry.size();
	}

	@Override
	public void removePosting(Term term, ObjectKey key) {
		PostingList postingList = invertedIndex.get(term);
		if(postingList!=null) {
			removeTermForObject(key, term);			
			postingList.remove(key);			
			if(postingList.getTermCount()==0) invertedIndex.remove(term);
			else invertedIndex.put(term,postingList);
		}
	}

	private void removeTermForObject(ObjectKey key, Term term) {
		MasterRegistryEntry masterRegistryEntry = masterRegistry.get(key);

		if(masterRegistryEntry == null) return;

		Set<Term> objectTerms = masterRegistryEntry.getTerms();

		if(objectTerms == null) return;

		objectTerms.remove(term);

		if(objectTerms.size()==0) masterRegistry.remove(key);
	}

	public void addToIndex(Term term, ObjectKey key, PostingMetadata metadata) {
		addToMasterRegistry(key, term);

		if(invertedIndex.containsKey(term)) {
			PostingList postingList = invertedIndex.get(term);
			postingList.add(key, metadata);
			invertedIndex.put(term, postingList);
		} else {			
			PostingList newPostingList = new PostingList(term);
			newPostingList.add(key, metadata);
			invertedIndex.put(term, newPostingList);
		}
	}

	private void addToMasterRegistry(ObjectKey key, Term term) {
		MasterRegistryEntry masterRegistryEntry = masterRegistry.get(key);

		Set<Term> objectTerms ;
		if(masterRegistryEntry == null ) {
			objectTerms = new HashSet<Term>();
			masterRegistry.put(key,new MasterRegistryEntry(objectTerms));
		} else {			
			objectTerms = masterRegistryEntry.getTerms();
		}

		objectTerms.add(term);			
	}


	@Override
	public PostingList getPostingList(Term term) {		
		return invertedIndex.get(term);
	}

	@Override
	public int getTermDictionarySize() {
		return invertedIndex.size();
	}

	@Override
	public void removeFromInvertedIndex(Term term) {
		invertedIndex.remove(term);
	}

	@Override
	public Iterator<Entry<ObjectKey, MasterRegistryEntry>> getMasterRegistryReadIterator() {
		return new MasterRegistryReadOnlyIterator(masterRegistry.entrySet().iterator());
	}

	@Override
	public void removeFromMasterRegistry(ObjectKey key) {
		masterRegistry.remove(key);
	}

	public static synchronized void closeIndexes() {
		checkGlobalInit();
		
		for (IndexId indexId : availableOpenIndexes.keySet()) {
			new BerkeleyIndex(indexId).close();
			availableOpenIndexes.remove(indexId);
		}
		
		availableIndexesDb.close();
		javaCatalog.close();
		env.close();
		globalInit = false;
	}

	void close() {
		invertedIndexDb.close();
		masterRegistryDb.close();
		availableOpenIndexes.remove(indexId);
	}

	private class MasterRegistryReadOnlyIterator implements Iterator<Entry<ObjectKey,MasterRegistryEntry>> {

		private final Iterator<Entry<ObjectKey,MasterRegistryEntry>> iterator;

		private Entry<ObjectKey,MasterRegistryEntry> current = null;

		public MasterRegistryReadOnlyIterator( Iterator<Entry<ObjectKey, MasterRegistryEntry>> iterator) {		
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {			
			return iterator.hasNext();
		}

		@Override
		public Entry<ObjectKey,MasterRegistryEntry> next() {
			current = iterator.next();
			return current;
		}

		@Override
		public void remove() {
			throw new RuntimeException("Remove not allowed on this iterator");
		}
	}

	public static BerkeleyIndex getDefaultIndex() {
		checkGlobalInit();
		return new BerkeleyIndex(IndexId.getDefaultIndexId());
	}

	public static BerkeleyIndex getIndex(IndexId indexId) {
		checkGlobalInit();
		return new BerkeleyIndex(indexId);
	}
	
	public boolean contains(Term term) {		
		return invertedIndex.containsKey(term);
	}

	public static void renewAllIndexes() {		
		checkGlobalInit();
		
		for (IndexId indexId : availableIndexes.keySet()) {
			new BerkeleyIndex(indexId).clear();			
		}
		
		availableIndexes.clear();		
		closeIndexes();
	}

	private void clear() {
		invertedIndex.clear();
		masterRegistry.clear();
	}

	public Iterator<Term> getTermDictionaryIterator() {
		return invertedIndex.keySet().iterator();
	}

	public static BerkeleyIndex newDefaultIndex() {
		checkGlobalInit();
		
		BerkeleyIndex newDefaultIndex = new BerkeleyIndex(IndexId.getDefaultIndexId());
		
		newDefaultIndex.clear();
		
		return newDefaultIndex;
	}

	public static void renewIndex(IndexId indexId) {
		checkGlobalInit();
		BerkeleyIndex berkeleyIndex = new BerkeleyIndex(indexId);
		berkeleyIndex.clear();
		berkeleyIndex.close();
	}
	
	public IndexId getIndexId() {
		return indexId;
	}
}
