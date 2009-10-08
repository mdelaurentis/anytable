import org.junit.Test;
import java.io.IOException;
import clojure.lang.IPersistentVector;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.Symbol;
import com.mdelaurentis.anytable.Anytable;

import static com.mdelaurentis.anytable.AnytableFactory.anytable;
import static org.junit.Assert.*;


public class AnytableTest {
 
  private static String[][] BREED_ROWS =
    {{"Beagle", "Hound", "Medium"},
     {"Basset Hound", "Hound", "Large"},
     {"Chihuahua", "", "Small"}};

  @Test
  public void testReadRows() throws IOException {
    
    Anytable at = null;
    String[][] valuesRead = new String[3][3];
    try {
      at = anytable("sample-data/breeds.tab").openReader();
      int rowNum = 0;
      for (IPersistentVector row : at.rows()) {
        valuesRead[rowNum][0] = (String)row.nth(0);
        valuesRead[rowNum][1] = (String)row.nth(1);
        valuesRead[rowNum][2] = (String)row.nth(2);
        rowNum++;
      }
      assertEquals(BREED_ROWS, valuesRead);
    } finally {
      if (at != null) {
        at.close();
      }
    }
  }

  @Test
  public void testReadRecords() throws IOException {
    
    Anytable at = null;
    String[][] valuesRead = new String[3][3];
    try {
      at = anytable("sample-data/breeds.tab").openReader();
      int rowNum = 0;
      for (IPersistentMap rec : at.records()) {
        valuesRead[rowNum][0] = (String)rec.valAt("breed");
        valuesRead[rowNum][1] = (String)rec.valAt("category");
        valuesRead[rowNum][2] = (String)rec.valAt("size");
        rowNum++;
      }
      assertEquals(BREED_ROWS, valuesRead);
    } finally {
      if (at != null) {
        at.close();
      }
    }
  }

  @Test
  public void testStringSpec() throws IOException {
    
    Anytable at = null;
    String[][] valuesRead = new String[3][3];
    try {
      at = anytable("{:type fixed-width " +
                    " :location \"sample-data/breeds.txt\"" + 
                    " :headers [\"breed\" \"category\" \"size\"]" +
                    " :widths [12 5 6]}").openReader();

      assertEquals("fixed-width", at.type());

      IPersistentVector headers = at.headers();
      assertEquals("breed",    headers.nth(0));
      assertEquals("category", headers.nth(1));
      assertEquals("size",   headers.nth(2));

      IPersistentVector widths = 
        (IPersistentVector) at.spec().valAt(Keyword.intern(Symbol.intern("widths")));
      assertEquals(12, widths.nth(0));
      assertEquals(5,  widths.nth(1));
      assertEquals(6,  widths.nth(2));

      int rowNum = 0;
      for (IPersistentMap rec : at.records()) {
        valuesRead[rowNum][0] = (String)rec.valAt("breed");
        valuesRead[rowNum][1] = (String)rec.valAt("category");
        valuesRead[rowNum][2] = (String)rec.valAt("size");
        rowNum++;
      }
      assertEquals(BREED_ROWS, valuesRead);
    } finally {
      if (at != null) {
        at.close();
      }
    }
  }

}