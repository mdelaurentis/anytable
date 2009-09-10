import org.junit.Test;
import java.io.IOException;
import clojure.lang.IPersistentVector;
import clojure.lang.IPersistentMap;
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
      at = anytable("test-input/breeds.tab").openReader();
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
      at = anytable("test-input/breeds.tab").openReader();
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