/*WAP in java to display multiplication table of 2*/

import java.util.Scanner;
class javaex3
{
	public static void main(String[] arg)
	{
		int num1;
		Scanner sc=new Scanner(System.in);

		System.out.println("enter a no. for which u want table");
		num1=sc.nextInt();
		for(int i=1;i<=10;i++)
		{
		System.out.println(num1*i);
		}
		
	}

}

ans=C:\Users\ADMIN\Desktop\supriya>javac javaex3.java

C:\Users\ADMIN\Desktop\supriya>java javaex3
enter a no. for which u want table
5
5
10
15
20
25
30
35
40
45
50