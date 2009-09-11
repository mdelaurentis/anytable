Anytable is a library and a set of command-line tools for working with tabular data in a variety of formats.  It provides a simple API for reading and writing data from tables stored in the following formats:

* Delimited flat files
* Fixed-width files
* Tables in a database through a JDBC connection
* More to come...

Library
=======

Anytable is written in Clojure, and also provides some interfaces to provide a convenient way of using the library from Java code.

Command-Line Tool
=================

Anytable has a command-line tool for performing simple operations on tables.

Interactive Help
----------------

You can use the command-line tool to get lots of help about the commands availables and the table types supported by the tool.  For example, get a list of types:

    $ anytable help types
    Table types are:
      hsqldb - HSQLDB table.
      jdbc-table - A table in a database.
      fixed-width - Fixed-width flat files.
      tab - Tab-delimited flat files.
      flat-file - Abstract type for any flat text file.
      vectors - An in-memory table made up of a vector of vectors.

To get a detailed description of a table type, do:

    $ anytable help fixed-width
    Fixed-width flat files.
    This type supports the following keys:
      :bounds - Start and end position of each column.  You can specify either this or :widths. 
      :widths - Vector of widths of each column (integers). 
      :headers - Vector of column headers. 
      :location - A URL or file locating the table. 
