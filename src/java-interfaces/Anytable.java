package com.mdelaurentis.anytable;

import java.io.Closeable;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentVector;
import clojure.lang.Var;
import clojure.lang.Symbol;
import clojure.lang.IFn;
import java.util.List;
import java.util.Map;

/**
 * Abstract representation of some tabular data.
 */
public interface Anytable extends Closeable {
  
  /**
   * Returns a vector of the headers (strings) for this table.  
   */
  public IPersistentVector headers();
  
  /**
   * Returns the anytable specification this object was constructed with.
   */
  public IPersistentMap spec();

  /**
   * Returns a new Anytable based on this table that is open for
   * reading.  You must call close() on the resulting Anytable after
   * you're done reading from it.
   */
  public Anytable openReader();

  /**
   * Returns a new Anytable based on this table that is open for
   * writing.  You must call close() on the resulting Anytable after
   * you're done writing to it.
   */     
  public Anytable openWriter();

  /**
   * Deletes the table.  For example, if this Anytable is backed by a
   * file on disk, this would cause the file to be deleted.  If it is
   * backed by a table in a database, the table would be dropped.  May
   * not be supported by some types of tables.
   */
  public Anytable delete();

  /**
   * Returns a sequence of rows from this table.  Each row is an
   * IPersistentVector.  Use this method when you want to access
   * values in each row by integer index.
   */
  public Iterable<IPersistentVector> rows();

  /**
   * Returns a sequence of records from this table, where each record
   * is an IPersistentMap with keys being the column names and values
   * being the value for each column for the current record.
   */
  public Iterable<IPersistentMap> records();

  /**
   * Writes a single row to the table.
   */
  public Anytable writeRow(List row);

  /**
   * Writes a single record to the table.
   */
  public Anytable writeRecord(Map record);
}