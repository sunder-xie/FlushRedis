package cm.redis.test;

import java.util.Iterator;
import java.util.TreeSet;

public class treesettest {
	public static void main(String[] args){
		TreeSet<String> treeSet=new TreeSet<String>();
		
		treeSet.add("2016-09-30");
		treeSet.add("2016-10-01");
		treeSet.add("2016-10-15");
		
		Iterator<String> iterator=treeSet.iterator();
		while(iterator.hasNext())
		{
			System.out.println(iterator.next().toString());
		}
	}
}
