/*write a program to accept detail of 10 employee,detail will be name and salary. Display total bonus givan to the employee,
criteria for bonus is

if salary is 10000then bonus is 30%
if salary is 50000then bonus is 20%
if salary is 100000then bonus is 10%  */


import java.util.Scanner;
class javaex11
{
	public static void main(String[] args)
	{
		String empNAME;
		int empSALARY=0;
		int i=0;
		int a=0;
		int b=0;
		int c=0;
		int bonus1;
		int bonus2;
		int bonus3;
		int totalbonus;
		Scanner sc=new Scanner(System.in);
		
		for(i=0;i<10;i++)
		{
			System.out.println("enter name");
			empNAME=sc.next();

			System.out.println("enter salary");	
			empSALARY=sc.nextInt();
		}
		if(empSALARY==10000)
		{
			bonus1=(10000*(30/100));
			a=a+bonus1;
			System.out.println("bonus1"+a);
		}
		if(empSALARY==50000)
		{
			bonus2=(10000*(30/100));
			b=b+bonus2;
			System.out.println("bonus2"+b);
		}
		if(empSALARY==100000)
		{
			bonus3=(10000*(30/100));
			c=c+bonus3;
			System.out.println("bonus3"+c);
		}
		totalbonus=a+b+c;
		System.out.println("total bonus="+totalbonus);
	}
}


C:\Users\ADMIN\Desktop\supriya>java javaex11
enter name
k
enter salary
10000
enter name
k200
enter salary
20000
enter name
k5000
enter salary
50000
enter name
k
enter salary
50000
enter name
k
enter salary
50
enter name
k
enter salary
4000
enter name
k
enter salary
20000
enter name
k
enter salary
600
enter name
k600
enter salary
500
enter name
k
enter salary
5000
total bonus=0