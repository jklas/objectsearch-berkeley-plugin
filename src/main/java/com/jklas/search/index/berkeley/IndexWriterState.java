package com.jklas.search.index.berkeley;

import com.jklas.search.index.IndexId;
import com.jklas.search.index.ObjectKey;
import com.jklas.search.index.PostingMetadata;
import com.jklas.search.index.Term;


public interface IndexWriterState {

    public void handleWrite(BerkeleyIndexWriter writer, Term term, ObjectKey key, PostingMetadata metadata);
    
    public void handleOpen(BerkeleyIndexWriter writer, IndexId indexId);
    
    public void handleClose(BerkeleyIndexWriter writer);

    public void handleDelete(BerkeleyIndexWriter berkeleyIndexWriter, ObjectKey objectKey);
}
