package bbejeck.mapred.coocurrance;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
    public void testCompareSpecialCharacter() throws Exception {
        WordPair p = new WordPair("chester","train");
        WordPair p1 = new WordPair("chester","apple");
        WordPair p2 = new WordPair("chester","*");
        WordPair p4 = new WordPair("apple","*");
        WordPair p5 = new WordPair("apple","dog");
        WordPair p6 = new WordPair("apple","cat");
        List<WordPair> list = new ArrayList<WordPair>();
        list.add(p);
        list.add(p1);
        list.add(p2);
        list.add(p6);
        list.add(p5);
        list.add(p4);
        Collections.sort(list);
        assertThat(list.get(0), is(p4));
        assertThat(list.get(1), is(p6));
        assertThat(list.get(2), is(p5));
        assertThat(list.get(3), is(p2));
        assertThat(list.get(4), is(p1));
        assertThat(list.get(5),is(p));
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
