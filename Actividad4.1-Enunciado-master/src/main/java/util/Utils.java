package util;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.neodatis.odb.Objects;

public class Utils {
	public static Date toDate(int year, int month, int day) {
		Calendar cal = Calendar.getInstance();
		cal.set(year, month, day, 0, 0, 0);
		Date date = cal.getTime();
		return date;

	}

	// https://docs.oracle.com/javase/tutorial/java/generics/genTypeInference.html#target_types
	public static <E> List<E> toList(Objects<E> empObjects) {
		List<E> empList = new ArrayList<>();
		while (empObjects.hasNext()) {
			E e = empObjects.next();
			empList.add(e);
		}
		return empList;
	}


}
