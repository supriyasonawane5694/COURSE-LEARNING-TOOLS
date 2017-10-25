/*WAP in java to accept mark of 3sbject and display pass  if mark greater than 50 and Display fail if it less than 50*/

import java.util.Scanner;
public class javaex4
{
	public static void main(String[] arg)
	{
		int maths,science,history,total=0;
		Scanner sc=new Scanner(System.in);

		System.out.println("enter mark in maths");
		maths=sc.nextInt();

		System.out.println("enter mark in sceince");
		science=sc.nextInt();

		System.out.println("enter mark in history");
		history=sc.nextInt();
		total=(maths+science+history);
		
		System.out.println("total"+total);
		if(total>50)
		{
			System.out.println("pass");
		}
		else
		{
			System.out.println("fail");
		}
	}
}

ans=

C:\Users\ADMIN\Desktop\supriya>javac javaex4.java

C:\Users\ADMIN\Desktop\supriya>java javaex4
enter mark in maths
23
enter mark in sceince
23
enter mark in history
23
total69
pass