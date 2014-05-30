package edu.buffalo.cse562.queryParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Test {
	static List<String> list1 = new ArrayList<>();
	static List<String> list2 = new ArrayList<>();
	static List<String> list3 = new ArrayList<>();
	static List<String> list4 = new ArrayList<>();
	
	static Iterator<String> iterator1; 
	static Iterator<String> iterator2; 
	static Iterator<String> iterator3; 
	static Iterator<String> iterator4;
	
	static List<Iterator<String>> listOfIterators = new ArrayList<>();
	static List<String> orderOfList = new ArrayList<>();
	static List<List<String>> listOfList = new ArrayList<>();
	static Iterator<String>[] arrayOfIterators = new Iterator[4];

/*	public static void method() {
		String lastIteratorName = orderOfList.get(orderOfList.size() - 1);
		Iterator<String> lastIterator = mapOfListIterators.get(lastIteratorName);
		while(lastIterator.hasNext()) {
			String value = lastIterator.next();
			resetInnerIterators(lastIteratorName);
		}
	}
*/	
	public static List<String> printValues(int num) {
		if(num == 0) {
			List<String> lastList = new ArrayList<>();
			while(arrayOfIterators[num].hasNext()) {
				lastList.add(arrayOfIterators[num].next());
			}
			return lastList;
		} else {
			resetInnerIterators(num);
			while(arrayOfIterators[num].hasNext()) {
				List<String> lastList = new ArrayList<>();
				lastList.add(arrayOfIterators[num].next());
				lastList.addAll(printValues(num - 1));
				System.out.println(lastList);
			}
				return null;
			}
	}
	
	public static void resetInnerIterators(int indexOfLastIterator) {
		System.out.println("Resetting for:" + indexOfLastIterator);
		for(int index = indexOfLastIterator; index>=0; index--) {
			List<String> list = listOfList.get(index);
			arrayOfIterators[index] = list.listIterator();
		}
	}
	
	public static void main1(String args[]) {
		list1.add("1a");
		list1.add("1b");
		list1.add("1c");

		list2.add("2a");
		list2.add("2b");
		list2.add("2c");

		list3.add("3a");
		list3.add("3b");
		list3.add("3c");

		list4.add("4a");
		list4.add("4b");
		list4.add("4c");
		
		listOfList.add(list1);
		listOfList.add(list2);
		listOfList.add(list3);
		listOfList.add(list4);
		
		iterator1 = list1.iterator(); 
		iterator2 = list2.iterator(); 
		iterator3 = list3.iterator(); 
		iterator4 = list4.iterator();

		
		listOfIterators.add(iterator1);
		listOfIterators.add(iterator2);
		listOfIterators.add(iterator3);
		listOfIterators.add(iterator4);
		
		orderOfList.add("list1");
		orderOfList.add("list2");
		orderOfList.add("list3");
		orderOfList.add("list4");
		
		arrayOfIterators[0] = iterator1;
		arrayOfIterators[1] = iterator2;
		arrayOfIterators[2] = iterator3;
		arrayOfIterators[3] = iterator4;
	List<String> allValues = printValues(3);
		
/*		for(String value : allValues) {
			System.out.println(value);
		}
*/		
	}
}
