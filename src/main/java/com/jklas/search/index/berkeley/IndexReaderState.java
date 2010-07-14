package com.jklas.search.index.berkeley;

import com.jklas.search.index.PostingList;
import com.jklas.search.index.Term;

public interface IndexReaderState {

    public PostingList handleRead(BerkeleyIndexReader reader, Term term);
    
    public void handleOpen(BerkeleyIndexReader reader);
    
    public void handleClose(BerkeleyIndexReader reader);

}
