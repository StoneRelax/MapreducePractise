package mapreducewordcount;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;







public class TheDriver {


	public static void main(String[] args) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		ClassDriver cdriver = new ClassDriver();
		cdriver.addClass("WordCount",WordCount.class,"use to count word");
		cdriver.addClass("MaxMinCount",MaxMinCount.class,"use to find max/min/count of a record name");
		cdriver.addClass("Mean",Mean.class,"use to find mean value");
		String classname = args[0];
		int returncode = cdriver.launch(classname,args);
		System.exit(returncode);
	}
	
	
	

	

	
}
