package com.jklas.search.index.berkeley;

import com.jklas.search.index.IndexId;
import com.jklas.search.index.MasterAndInvertedIndexReader;
import com.jklas.search.index.PostingList;
import com.jklas.search.index.Term;

public class BerkeleyIndexReader implements MasterAndInvertedIndexReader {

	private BerkeleyIndex index;

	private IndexReaderState state;

	public BerkeleyIndexReader(IndexId indexId) {
		setState(new ClosedReaderState());
		index = BerkeleyIndex.getIndex(indexId);
		if(index ==null)
			throw new IllegalArgumentException("Can't read index "+indexId+" since it does not exists");
	}

	public BerkeleyIndexReader() {
		this( IndexId.getDefaultIndexId() );
	}
	
	@Override
	public void close() {
		state.handleClose(this);
	}

	@Override
	public int getIndexedObjectCount() {
		return index.getObjectCount();
	}

	@Override
	public void open() {
		open( index.getIndexId() );
	}

	@Override
	public PostingList openReadAndClose(Term term) {
		return openReadAndClose(IndexId.getDefaultIndexId(), term);
	}

	@Override
	public PostingList openReadAndClose(IndexId indexId, Term term) {
		state.handleOpen(this);
		try {
			return state.handleRead(this, term);
		} finally {
			state.handleClose(this);
		}
	}
	
	@Override
	public PostingList read(Term term) {
		return state.handleRead(this, term);
	}

	void openWhenClosed() {
		openWhenClosed( index.getIndexId() );
	}

	void openWhenClosed(IndexId indexId) {
		this.index = BerkeleyIndex.getIndex( indexId );
	}
	
	void openWhenOpen() {
		throw new IllegalStateException("Index is already open");
	}

	void closeWhenClosed() {
		throw new IllegalStateException("Index is already closed");	
	}

	void closeWhenOpen() {
		this.index = null;
	}

	public PostingList readWhenClosed() {
		throw new IllegalStateException("Index is closed");
	}

	public void setState(IndexReaderState newState) {
		this.state = newState;
	}

	public PostingList readWhenOpen(Term term) {
		return index.getPostingList(term);
	}

	@Override
	public void open(IndexId indexId) {
		state.handleOpen(this);
	}

	@Override
	public boolean isOpen() {		
		return state.getClass().isAssignableFrom(OpenWriterState.class);
	}

	

}
