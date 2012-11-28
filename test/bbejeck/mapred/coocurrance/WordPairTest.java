package bbejeck.mapred.coocurrance;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
/**
 * User: Bill Bejeck
 * Date: 11/26/12
 * Time: 9:30 PM
 */
public class WordPairTest {

    private WordPair wp = new WordPair("test","foo");
    private WordPair wp1 = new WordPair("test","bar");

    @Test
    public void testCompareTo() throws Exception {
          assertThat(wp.compareTo(wp),is(0));
          assertTrue(wp.compareTo(wp1) > 0);
    }

    @Test
    public void testEquals() throws Exception {
         assertThat(wp.equals(wp),is(true));
         assertThat(wp1.equals(wp),is(false));
    }

    @Test
    public void testHashCode() throws Exception {
        assertThat(wp.hashCode(),is(wp.hashCode()));
    }

    @Test
    public void testSetWord() throws Exception {
       String expected = "baz";
       wp.setWord(expected);
       assertThat(wp.getWord().toString(),is(expected));
    }

    @Test
    public void testSetNeighbor() throws Exception {
        String expected = "baz";
        wp.setNeighbor(expected);
        assertThat(wp.getNeighbor().toString(),is(expected));
    }


}
