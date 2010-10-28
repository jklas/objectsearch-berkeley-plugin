package com.jklas.search.index;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.jklas.search.annotations.IndexSelector;
import com.jklas.search.annotations.Indexable;
import com.jklas.search.annotations.SearchField;
import com.jklas.search.annotations.SearchId;
import com.jklas.search.configuration.AnnotationConfigurationMapper;
import com.jklas.search.exception.IndexObjectException;
import com.jklas.search.exception.SearchEngineMappingException;
import com.jklas.search.index.berkeley.BerkeleyGlobalPropertyEditor;
import com.jklas.search.index.berkeley.BerkeleyIndex;
import com.jklas.search.index.berkeley.BerkeleyIndexReaderFactory;
import com.jklas.search.index.berkeley.BerkeleyIndexWriterFactory;
import com.jklas.search.indexer.DefaultIndexerService;
import com.jklas.search.indexer.pipeline.DefaultIndexingPipeline;


public class BerkeleyDbIndexTest {

	@SuppressWarnings("unused")
	@Indexable
	private class Customer{
		@SearchId
		private int id = 1;

		@SearchField(accessByGet=true)
		private String name = "JULI";

		public String getName() {
			return name;
		}
	}

	@Before
	public void resetIndexes() {
		new BerkeleyGlobalPropertyEditor().setBaseDir("src/test/resources");
		BerkeleyIndex.renewAllIndexes();
	}

	@After
	public void closeIndexes() {
		BerkeleyIndex.closeIndexes();
	}

	@AfterClass
	public static void clean() {
		File[] listFiles = new File("src/test/resources/").listFiles();

		for (File file : listFiles) {
			file.delete();
		}
	}

	@Test
	public void testIndexIsRenewedWhenNewIsCalled() {		
		BerkeleyIndex berkeleyIndex = BerkeleyIndex.getDefaultIndex();

		Term term = new Term("JULI");

		ObjectKey key = new ObjectKey(Customer.class,new Integer(0));
		berkeleyIndex.addToIndex(term, key,PostingMetadata.NULL);

		Assert.assertTrue(berkeleyIndex.getTermDictionarySize()==1);				

		berkeleyIndex = BerkeleyIndex.newDefaultIndex();

		Assert.assertTrue(berkeleyIndex.getTermDictionarySize()==0);
	}

	@Test
	public void testAddedPostingIsOnIndex() {		
		BerkeleyIndex berkeleyIndex = BerkeleyIndex.getDefaultIndex();

		Term term = new Term("JULI");

		ObjectKey key = new ObjectKey(Customer.class,new Integer(0));
		berkeleyIndex.addToIndex(term, key,PostingMetadata.NULL);

		Assert.assertTrue(berkeleyIndex.contains(term));		

		Assert.assertTrue(berkeleyIndex.getPostingList(term).contains(key));
	}

	@Test
	public void testDeletedPostingIsNotOnIndex() {		
		BerkeleyIndex berkeleyIndex = BerkeleyIndex.getDefaultIndex();

		Term term = new Term("JULI");
		ObjectKey key = new ObjectKey(Customer.class,new Integer(0));
		berkeleyIndex.addToIndex(term, key,PostingMetadata.NULL);

		Assert.assertTrue(berkeleyIndex.contains(term));

		berkeleyIndex.removeFromInvertedIndex(term);

		Assert.assertFalse(berkeleyIndex.contains(term));
	}

	@Test
	public void testPostingListContainsAllPostings() {		
		BerkeleyIndex berkeleyIndex = BerkeleyIndex.getDefaultIndex();
		Set<ObjectKey> auxSet = new HashSet<ObjectKey>();

		Term term = new Term("JULI");

		for (int i = 0; i < 3; i++) {			
			PostingMetadata pm = PostingMetadata.NULL;
			ObjectKey oik = new ObjectKey(Customer.class, new Integer(i));
			berkeleyIndex.addToIndex(term, oik, pm);
			ObjectKey posting = new ObjectKey(oik);
			auxSet.add(posting);
		}

		for (Entry<ObjectKey, PostingMetadata> postingEntry : berkeleyIndex.getPostingList(new Term("JULI"))) {
			Assert.assertTrue(auxSet.contains(postingEntry.getKey()));					
		}
	}


	@Test
	public void testTfEqualsNineWhen3PostingsWith3ocurrencesAreAddedToIndex() throws SecurityException, NoSuchFieldException {		
		BerkeleyIndex berkeleyIndex = BerkeleyIndex.getDefaultIndex();

		Term term = new Term("JULI");

		Field nameField = Customer.class.getDeclaredField("name");

		Map<Field,Serializable> fieldValueMap = new HashMap<Field,Serializable>();
		Map<Field,Integer> fieldTfMap = new HashMap<Field,Integer>();

		fieldTfMap.put(nameField, 3);

		for (int i = 0; i < 3; i++) {
			PostingMetadata pm = new PostingMetadata(fieldValueMap, fieldTfMap);

			ObjectKey oik = new ObjectKey(Customer.class, new Integer(i));

			berkeleyIndex.addToIndex(term, oik, pm);	
		}


		Assert.assertEquals(9,berkeleyIndex.getPostingList(term).getTermCount());		
	}

	@Test
	public void testPostingListIteratorDecrementsTf() throws SecurityException, NoSuchFieldException {	
		PostingList postingList = new PostingList(new Term("DUMMY"));

		Map<Field,Serializable> fieldValueMap = new HashMap<Field,Serializable>();
		Map<Field,Integer> fieldTfMap = new HashMap<Field,Integer>();

		Field nameField = Customer.class.getDeclaredField("name");

		fieldTfMap.put(nameField, 3);

		for (int i = 0; i < 3; i++) {
			PostingMetadata pm = new PostingMetadata(fieldValueMap, fieldTfMap);
			ObjectKey oik = new ObjectKey(Customer.class, new Integer(i));
			postingList.add(oik,pm);
		}

		Iterator<Entry<ObjectKey, PostingMetadata>> iterator = postingList.iterator();
		Assert.assertEquals(9,postingList.getTermCount());
		iterator.next();
		iterator.remove();
		Assert.assertEquals(6,postingList.getTermCount());
		iterator.next();
		iterator.remove();
		Assert.assertEquals(3,postingList.getTermCount());
		iterator.next();
		iterator.remove();
		Assert.assertEquals(0,postingList.getTermCount());
	}

	@Test
	public void testAddWhileIteratingDoesNotMakeNewElementsVisible() {		
		PostingList postingList = new PostingList(new Term("DUMMY"));
		Iterator<Entry<ObjectKey, PostingMetadata>> iterator = postingList.iterator();

		for (int i = 0; i < 3; i++) {			
			PostingMetadata pm = new PostingMetadata(null,null);
			ObjectKey oik = new ObjectKey(Customer.class, new Integer(i));
			postingList.add(oik,pm);

			Assert.assertFalse(iterator.hasNext());
			Assert.assertTrue(postingList.iterator().hasNext());
		}		
	}

	@Test
	public void testDictionarySizeIsUpdatedOk() {
		BerkeleyIndex berkeleyIndex = BerkeleyIndex.getDefaultIndex();

		ObjectKey key = new ObjectKey(Customer.class,new Integer(0));

		Assert.assertEquals(0,berkeleyIndex.getTermDictionarySize());
		berkeleyIndex.addToIndex(new Term("A"), key, PostingMetadata.NULL);
		Assert.assertEquals(1,berkeleyIndex.getTermDictionarySize());
		berkeleyIndex.addToIndex(new Term("B"), key, PostingMetadata.NULL);
		Assert.assertEquals(2,berkeleyIndex.getTermDictionarySize());
		berkeleyIndex.addToIndex(new Term("C"), key, PostingMetadata.NULL);
		Assert.assertEquals(3,berkeleyIndex.getTermDictionarySize());
		berkeleyIndex.removeFromInvertedIndex(new Term("A"));
		Assert.assertEquals(2,berkeleyIndex.getTermDictionarySize());
		berkeleyIndex.removeFromInvertedIndex(new Term("B"));
		Assert.assertEquals(1,berkeleyIndex.getTermDictionarySize());
		berkeleyIndex.removeFromInvertedIndex(new Term("C"));		
		Assert.assertEquals(0,berkeleyIndex.getTermDictionarySize());
	}

	@Test
	public void testDictionaryHavesAllAddedKeys() {
		BerkeleyIndex berkeleyIndex = BerkeleyIndex.getDefaultIndex();
		HashSet<Term> auxSet = new HashSet<Term>();
		HashSet<ObjectKey> auxKeySet = new HashSet<ObjectKey>();

		ObjectKey key = new ObjectKey(Customer.class,new Integer(0));

		auxKeySet.add(key);

		Term term = new Term("A");
		berkeleyIndex.addToIndex(term, key, PostingMetadata.NULL);
		auxSet.add(term);

		term = new Term("B");
		berkeleyIndex.addToIndex(term, key, PostingMetadata.NULL);
		auxSet.add(term);

		term = new Term("C");
		berkeleyIndex.addToIndex(term, key, PostingMetadata.NULL);
		auxSet.add(term);		

		for (Iterator<Term> iterator = berkeleyIndex.getTermDictionaryIterator(); iterator.hasNext();) {
			Assert.assertTrue(auxSet.contains(iterator.next()));			
		}

		for (Iterator<Entry<ObjectKey,MasterRegistryEntry>> iterator = berkeleyIndex.getMasterRegistryReadIterator(); iterator.hasNext();) {
			Entry<ObjectKey,MasterRegistryEntry> entry= (Entry<ObjectKey,MasterRegistryEntry>) iterator.next();
			Assert.assertTrue(auxKeySet.contains(entry.getKey()));
		}
	}

	@Test
	public void testDictionaryAndMasterRegistryDeletesKeys() {
		BerkeleyIndex berkeleyIndex = BerkeleyIndex.getDefaultIndex();
		ObjectKey key = new ObjectKey(Customer.class,new Integer(0));

		Term term = new Term("A");
		berkeleyIndex.addToIndex(term, key, PostingMetadata.NULL);

		term = new Term("B");
		berkeleyIndex.addToIndex(term, key, PostingMetadata.NULL);

		term = new Term("C");
		berkeleyIndex.addToIndex(term, key, PostingMetadata.NULL);

		berkeleyIndex.removeFromInvertedIndex(new Term("A"));
		berkeleyIndex.removeFromInvertedIndex(new Term("B"));
		berkeleyIndex.removeFromInvertedIndex(new Term("C"));

		Assert.assertTrue(berkeleyIndex.getTermDictionarySize()==0);

	}

	@Test
	public void testDeleteLastPostingDeletesPostingListAndMasterRegistryEntry() {
		BerkeleyIndex berkeleyIndex = BerkeleyIndex.getDefaultIndex();

		ObjectKey key = new ObjectKey(Customer.class,new Integer(0));

		Term term = new Term("A");

		berkeleyIndex.addToIndex(term, key, PostingMetadata.NULL);
		berkeleyIndex.removePosting(term, key);

		Assert.assertNull(berkeleyIndex.getPostingList(term));

		Iterator<Entry<ObjectKey,MasterRegistryEntry>> it = berkeleyIndex.getMasterRegistryReadIterator();

		while (it.hasNext()) {
			Assert.fail();
		}		
	}

	@Test
	public void testIndexWriterAndReaderFactories() {
		ObjectKey key = new ObjectKey(Customer.class,new Integer(0));

		MasterAndInvertedIndexWriter indexWriter = BerkeleyIndexWriterFactory.getInstance().getIndexWriter();		
		indexWriter.open();
		indexWriter.write(new Term("JULI"), key, PostingMetadata.NULL);
		indexWriter.close();

		MasterAndInvertedIndexReader indexReader = BerkeleyIndexReaderFactory.getInstance().getIndexReader();

		indexReader.open();
		PostingList postingList = indexReader.read(new Term("JULI"));
		indexReader.close();

		Assert.assertEquals(key, postingList.iterator().next().getKey());
	}

	@SuppressWarnings("unused")
	@Indexable
	private class DummyForIndexA {
		@SearchId public int id =1;

		@IndexSelector public String indexToSelect = "A";

		@SearchField public String name="MARTIN";

	}

	@SuppressWarnings("unused")
	@Indexable
	private class DummyForIndexB {
		@SearchId public int id =1;

		@IndexSelector public String indexToSelect = "B";

		@SearchField public String name="MARTIN";

	}

	@Test
	public void testDifferentIndexesAreIsolated() throws SearchEngineMappingException, IndexObjectException {
		DummyForIndexA entityA = new DummyForIndexA();
		DummyForIndexB entityB = new DummyForIndexB();

		AnnotationConfigurationMapper.configureAndMap(entityA);
		AnnotationConfigurationMapper.configureAndMap(entityB);

		DefaultIndexingPipeline indexingPipeline = new DefaultIndexingPipeline();		
		DefaultIndexerService idxService = new DefaultIndexerService(indexingPipeline, BerkeleyIndexWriterFactory.getInstance());

		idxService.create(entityA);
		idxService.create(entityB);

		BerkeleyIndexReaderFactory readerFactory = BerkeleyIndexReaderFactory.getInstance();

		MasterAndInvertedIndexReader indexReaderForIndexA = readerFactory.getIndexReader(new IndexId("A"));
		MasterAndInvertedIndexReader indexReaderForIndexB = readerFactory.getIndexReader(new IndexId("B"));

		try {
			indexReaderForIndexA.open();
			indexReaderForIndexB.open();

			Assert.assertNotNull(indexReaderForIndexA.read(new Term("MARTIN")));
			Assert.assertEquals(indexReaderForIndexA.getIndexedObjectCount(),1);

			Assert.assertNotNull(indexReaderForIndexB.read(new Term("MARTIN")));
			Assert.assertEquals(indexReaderForIndexB.getIndexedObjectCount(),1);			
		} finally {
			indexReaderForIndexA.close();
			indexReaderForIndexB.close();
		}

	}

}
