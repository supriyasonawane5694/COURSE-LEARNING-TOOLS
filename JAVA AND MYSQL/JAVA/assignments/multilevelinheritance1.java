/*multilevel inheritance*/


import java.util.Scanner;
class A	
	{
		public void say1()
		{
		System.out.println("hello 1");
		}
	}
class B extends A
	{
		
		public void say2()
		{
		System.out.println("hello 2");
		}
	}
class C extends B
	{
		public void say3()
		{
		System.out.println("hello 3");
		}
	}
	

public class multilevelinheritance1
{
	public static void main(String[] arg)
	{
		 C obj=new C();
		obj.say3();
		obj.say2();
		obj.say1();
		
	}
}
