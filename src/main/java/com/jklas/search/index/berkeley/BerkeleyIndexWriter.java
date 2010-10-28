package com.jklas.search.index.berkeley;

import com.jklas.search.index.IndexId;
import com.jklas.search.index.MasterAndInvertedIndexWriter;
import com.jklas.search.index.ObjectKey;
import com.jklas.search.index.PostingMetadata;
import com.jklas.search.index.Term;

public class BerkeleyIndexWriter implements MasterAndInvertedIndexWriter {

	private IndexWriterState state;

	private BerkeleyIndex berkeleyIndex ;

	public BerkeleyIndexWriter() {
		this.state = new ClosedWriterState();		
	}

	void setState(IndexWriterState newState) {
		this.state = newState;
	}

	@Override
	public void close() {
		state.handleClose(this);
	}

	@Override
	public void openDeleteAndClose(IndexId indexId, ObjectKey objectKey) {
		state.handleOpen(this, indexId);
		try {
			state.handleDelete(this, objectKey);
		} finally {
			state.handleClose(this);
		}
	}

	@Override
	public void openWriteAndClose(Term term, ObjectKey key, PostingMetadata metadata) {
		openWriteAndClose(IndexId.getDefaultIndexId(), term, key, metadata);
	}

	@Override
	public void openWriteAndClose(IndexId indexId, Term term, ObjectKey key, PostingMetadata metadata) {
		state.handleOpen(this, indexId);
		try {
			state.handleWrite(this, term, key, metadata);
		} finally {
			state.handleClose(this);
		}
	}
	
	@Override
	public void write(Term term, ObjectKey key, PostingMetadata metadata) {
		state.handleWrite(this, term, key, metadata);
	}

	void openWhenClosed(IndexId indexId) {
		this.berkeleyIndex = BerkeleyIndex.getIndex(indexId);
	}

	void openWhenOpen(IndexId indexId) {
		throw new IllegalStateException("Index is already open");
	}

	void closeWhenClosed() {
		throw new IllegalStateException("Index is already closed");	
	}

	void closeWhenOpen() {
		berkeleyIndex.close();
		this.berkeleyIndex = null;
	}

	void writeWhenClosed() {
		throw new IllegalStateException("Can't write on a closed index");
	}

	void writeWhenOpen(Term term, ObjectKey key, PostingMetadata metadata) {
		this.berkeleyIndex.addToIndex(term, key, metadata);
	}

	void deleteWhenOpen(ObjectKey objectKey) {
		this.berkeleyIndex.removeFromMasterRegistry(objectKey);
	}

	void deleteWhenClosed() {
		throw new IllegalStateException("Can't delete on a closed index");
	}

	@Override
	public void open() {
		open(IndexId.getDefaultIndexId());
	}

	@Override
	public void open(IndexId indexId) {
		state.handleOpen(this, indexId);
	}

	@Override
	public void delete(ObjectKey objectKey) {
		state.handleDelete(this, objectKey);
	}

}
