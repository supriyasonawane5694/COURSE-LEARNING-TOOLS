/*WAP to accept 5 no. and display the max no.*/

import java.util.Scanner;
public class javaex2
{
	public static void main(String [] args)
	{
		int a,maximum=0;
		Scanner sc=new Scanner(System.in);
		int i=0;
		while(i<5)
		{
			System.out.println("enter a number");
			a=sc.nextInt();
			if(a>maximum)
			{
				maximum=a;
			}
			System.out.println("max number is" +maximum);
		}
		i++;
		
	}
	
}

ans=
C:\Users\ADMIN\Desktop\supriya>java javaex2
enter a number
2
max number is2
enter a number
3
max number is3
enter a number
3
max number is3
enter a number
3
max number is3
enter a number
4
max number is4
enter a number