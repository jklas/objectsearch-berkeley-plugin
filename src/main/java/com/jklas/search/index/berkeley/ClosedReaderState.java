package com.jklas.search.index.berkeley;

import com.jklas.search.index.PostingList;
import com.jklas.search.index.Term;


public class ClosedReaderState implements IndexReaderState {

	@Override
	public void handleClose(BerkeleyIndexReader reader) {
		reader.closeWhenClosed();
	}

	@Override
	public void handleOpen(BerkeleyIndexReader reader) {
		reader.openWhenClosed();
		reader.setState(new OpenReaderState());
	}

	@Override
	public PostingList handleRead(BerkeleyIndexReader reader, Term term) {		
		return reader.readWhenClosed();
	}

}
