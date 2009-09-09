package com.mdelaurentis.anytable;

import clojure.lang.ISeq;
import clojure.lang.IPersistentVector;

public interface AnytableReader extends Closeable {
  
  public IPersistentVector headers();

  public ISeq rowSeq();

  public ISeq recordSeq();

}