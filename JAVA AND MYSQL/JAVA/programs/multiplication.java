//write a program to display multiplication table of 2.
import java.util.Scanner;
public class multiplication
{
public static void main(String[]args)
{
int num1;
Scanner sc=new Scanner(System.in);
System.out.println("enter a no for which u want table");
num1=sc.nextInt();
for(int i=1;i<=10;i++)
{
System.out.println(num1*i);
}
}
}

output:

enter a no for which u want table
2
2
4
6
8
10
12
14
16
18
20
