package com.jklas.search.index.berkeley;

import com.jklas.search.index.PostingList;
import com.jklas.search.index.Term;

public class OpenReaderState implements IndexReaderState {

	@Override
	public void handleClose(BerkeleyIndexReader reader) {
		reader.closeWhenOpen();
		reader.setState(new ClosedReaderState());
	}

	@Override
	public void handleOpen(BerkeleyIndexReader reader) {
		reader.openWhenOpen();
	}

	@Override
	public PostingList handleRead(BerkeleyIndexReader reader, Term term) {
		return reader.readWhenOpen(term);
	}
}
