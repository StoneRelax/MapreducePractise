package mapreducewordcount;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;



public class ClassDriver{
	
	//Map of key = Class name and value = Description we use
	HashMap<String,ClassDescrition> classes = new HashMap<String,ClassDescrition>();
	public void addClass(String name , Class<?> mainclass , String description) throws NoSuchMethodException, SecurityException{
		ClassDescrition cd = new ClassDescrition(mainclass,description);
		classes.put(name,cd);
	}
	
	public int launch(String name ,String[] args) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		ClassDescrition cd = classes.get(name);
		String[] newargs = new String[args.length-1];
		if (null != cd){
			System.out.println(args.length);
			for (int i = 1;i < args.length;i++){
				newargs[i-1] = args[i];
			}
			System.out.println(cd.mainclass.toString());
			cd.mainclass.invoke(null ,new Object[]{newargs});
		}else {
			return 1;
		}
		return 0;
	}
	
	public static class ClassDescrition{
		public Method mainclass ;
		String description ;
		
		static final Class<?>[] paraTypes = new Class<?>[] {String[].class};
		
		public ClassDescrition(Class<?> mainclass , String description) throws NoSuchMethodException, SecurityException{
			this.mainclass = mainclass.getMethod("main",paraTypes);
			this.description = description ;
		}
	}
	
	
	
}