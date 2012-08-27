package skewtune.utils;

import static org.junit.Assert.*;
import static skewtune.utils.Itertools.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import skewtune.utils.Itertools;

public class ItertoolsTest extends Itertools {
    @Test
    public final void filterTest() {
        List<Integer> arr = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        Filter<Integer> f = new Filter<Integer>(arr, new Predicate<Integer>() {
            @Override
            public boolean eval(Integer value) {
                return value > 5;
            }});
        
        List<Integer> result = new ArrayList<Integer>();
        for ( Integer v : f ) {
            result.add(v);
        }
        
        assertTrue(result.equals( Arrays.asList(6,7,8,9) ));
    }
    
    @Test
    public final void filterTestEmptyInput() {
        List<Integer> arr = Collections.emptyList();
        Filter<Integer> f = new Filter<Integer>(arr, new Predicate<Integer>() {
            @Override
            public boolean eval(Integer value) {
                return value > 5;
            }});
        
        List<Integer> result = new ArrayList<Integer>();
        for ( Integer v : f ) {
            result.add(v);
        }
        
        assertTrue(result.equals( Collections.emptyList() ));
    }

    @Test
    public final void groupByTest() {
        List<Integer> arr = Arrays.asList(2,3,3,4,6,7,9);
        GroupBy<Integer,Integer> groups = new GroupBy<Integer,Integer>(arr, new KeyFunc<Integer,Integer>() {
            @Override
            public Integer eval(Integer v) {
                return v/3;
            }});

        StringBuilder buf = new StringBuilder();
        for ( Group<Integer,Integer> group : groups ) {
            buf.setLength(0);
            buf.append(group.group()).append(':');
            for ( Integer v : group ) {
                buf.append(v).append(',');
            }
            buf.setLength(buf.length()-1);
            
            System.out.println(buf.toString());
        }
    }
    
    @Test
    public final void priorityQueueTest() {
        List<Integer> arr = Arrays.asList(2,3,3,4,6,7,9);
        PriorityQueue<Integer> q = new PriorityQueue<Integer>();
        
        // first add random item
        q.add(9);
        q.add(7);
        q.add(15);
        q.add(3);
        q.add(21);
        
        
        // iterate 3 times
        Iterator<Integer> i = q.iterator();
        assertEquals(new Integer(3),i.next());
        assertEquals(new Integer(7),i.next());
        
        // add more item
        q.add(9);
        q.add(8);
        q.add(11);
        
        // iterate till the end
        i = q.iterator();
        assertEquals(new Integer(8),i.next());
        assertEquals(new Integer(9),i.next());
        assertEquals(new Integer(9),i.next());
        assertEquals(new Integer(11),i.next());
        assertEquals(new Integer(15),i.next());
        assertEquals(new Integer(21),i.next());

    }
}