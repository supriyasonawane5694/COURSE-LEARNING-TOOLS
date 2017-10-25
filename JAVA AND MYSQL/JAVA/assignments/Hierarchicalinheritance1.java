/* inheritance with contructor*/


import java.util.Scanner;
class A	
	{
		int a,b,c,d;
		public void say1()
		{
		Scanner sc=new Scanner(System.in);
		System.out.println("enter two no");
		a=sc.nextInt();

		b=sc.nextInt();
		c=a+b;
		System.out.println("sum"+c);
		}
	}
class B extends A
	{
		
		public void say2()
		{
		 d=c+2;
		System.out.println("display c"+d);
		}
	}
class C extends B
	{
		public void say3()
		{
		System.out.println("hello 3");
		}
	}
	

public class constructorinheritance1
{
	public static void main(String[] arg)
	{
		
		B obj=new B();
		obj.say1();
		obj.say2();

		
	}
}
