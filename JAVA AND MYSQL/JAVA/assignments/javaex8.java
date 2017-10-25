/*Write a Menu driven program for function addition, substraction,multiplication,divide*/
import java.util .Scanner;
public class javaex8
{
public void Add(int a,int b)
{
int sum=a+b;
System.out.println("Addition is="+sum);
}

public void Sub(int a,int b)
{
int sum=a-b;
System.out.println("Subtraction is="+sum);
}

public void Divide(int a,int b)
{
int sum=a/b;
System.out.println("Division is="+sum);
}

public void Multi(int a,int b)
{
int sum=a*b;
System.out.println("Multiplication is="+sum);
}

public static void main(String[] args)
{
javaex8 obj=new javaex8();
Scanner sc=new Scanner(System.in);
int num1=0;
int num2=0;
int ch=0;
do
{
System.out.println("\n1.Addition \n2.Subtraction \n3.Division \n4.Multiplication \n5.Exit");

System.out.println("Enter your choice=");
ch=sc.nextInt();
switch(ch)
{
case 1:
System.out.println("enter 1st no:");
num1=sc.nextInt();
System.out.println("enter 2nd no:");
num2=sc.nextInt();
obj.Add(num1,num2);
break;

case 2:
System.out.println("enter 1st no:");
num1=sc.nextInt();
System.out.println("enter 2nd no:");
num2=sc.nextInt();
obj.Sub(num1,num2);
break;

case 3:
System.out.println("enter 1st no:");
num1=sc.nextInt();
System.out.println("enter 2nd no:");
num2=sc.nextInt();
obj.Divide(num1,num2);
break;

case 4:
System.out.println("enter 1st no:");
num1=sc.nextInt();
System.out.println("enter 2nd no:");
num2=sc.nextInt();
obj.Multi(num1,num2);
break;

case 5:
break;

default:
System.out.println("Invalid Choice");
break;
}
}while(ch==6);
}
}

output:

C:\Users\ADMIN\Desktop\supriya>javac javaex8.java

C:\Users\ADMIN\Desktop\supriya>java javaex8

1.Addition
2.Subtraction
3.Division
4.Multiplication
5.Exit
Enter your choice=
1
enter 1st no:
12
enter 2nd no:
23
Addition is=35