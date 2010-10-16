package com.jklas.search.index.berkeley;

import com.jklas.search.index.IndexId;
import com.jklas.search.index.ObjectKey;
import com.jklas.search.index.PostingMetadata;
import com.jklas.search.index.Term;

public class OpenWriterState implements IndexWriterState {

	@Override
	public void handleClose(BerkeleyIndexWriter writer) {
		writer.closeWhenOpen();
		writer.setState(new ClosedWriterState());
	}

	@Override
	public void handleWrite(BerkeleyIndexWriter writer, Term term, ObjectKey key, PostingMetadata metadata) {
		writer.writeWhenOpen(term, key, metadata);
	}

	@Override
	public void handleDelete(BerkeleyIndexWriter writer, ObjectKey objectKey) {
		writer.deleteWhenOpen(objectKey);
	}

	@Override
	public void handleOpen(BerkeleyIndexWriter writer, IndexId indexId) {
		writer.openWhenOpen(indexId);	
	}
}
