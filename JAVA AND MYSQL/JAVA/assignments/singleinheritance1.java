/*single inheritance*/


import java.util.Scanner;
class A	
	{
		public void say()
		{
		System.out.println("hello");
		}
	}
class B extends A
	{
		
		public void say1(){
		super.say();
		System.out.println("hello1");
		}
	}
	

public class singleinheritance1
{
	public static void main(String[] arg)
	{
		 B obj=new B();
		obj.say1();
		
		
	}
}
