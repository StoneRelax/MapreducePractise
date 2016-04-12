package mapreducewordcount;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.sun.org.apache.bcel.internal.generic.RETURN;





public class TheDriver {


	public static void main(String[] args) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		ClassDriver cdriver = new ClassDriver();
		cdriver.addClass("WordCount",WordCount.class,"use to count word");
		String classname = args[0];
		int returncode = cdriver.launch(classname,args);
		System.exit(returncode);
	}
	
	
	

	

	
}
