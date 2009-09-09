package com.mdelaurentis.anytable;

import java.io.Closeable;
import java.io.File;
import java.net.URI;

import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentVector;
import clojure.lang.Var;
import clojure.lang.Symbol;
import clojure.lang.IFn;

import com.mdelaurentis.anytable.AnytableImpl;

public class AnytableFactory {
  
  public static Anytable anytable(IPersistentMap spec) {
    return anytable((Object)spec);
  }

  public static Anytable anytable(String spec) {
    return anytable((Object)spec);
  }

  public static Anytable anytable(File spec) {
    return anytable((Object)spec);
  }

  public static Anytable anytable(URI spec) {
    return anytable((Object)spec);
  }

  public static Anytable anytable(Object spec) {
    return new AnytableImpl(spec);
  }
  
}