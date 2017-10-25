/*wap to accept 10 no. and display their sum*/

import java.util.Scanner;
public class javaex1
{
	public static void main(String[] args)
	{
		int a, total=0;
		Scanner sc=new Scanner(System.in);
		int i;
		for(i=0;i<10;i++)
		{
			System.out.println("enter number=");
			a=sc.nextInt();
			total=total+a;
		}
		System.out.println("sum of 10no. is"+total);
	}

}

ans=
C:\Users\ADMIN\Desktop\supriya>java javaex1
enter number=
2
enter number=
2
enter number=
2
enter number=
2
enter number=
2
enter number=
2
enter number=
2
enter number=
2
enter number=
2
enter number=
2
sum of 10no. is20