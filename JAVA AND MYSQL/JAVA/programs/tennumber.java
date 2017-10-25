//write a program to accept 10 number and display sum of 10 number.
import java.util.Scanner;
public class tennumber
{
public static void main(String[]args)
{
int num,sum;
sum=0;
Scanner sc=new Scanner(System.in);
int i=1;
while(i<=10)
{
System.out.println("enter the number");
num=sc.nextInt();
sum=sum+num;
i++;
}
System.out.println("the sum of ten number is"+sum);
}
}



output:

enter the number
10
enter the number
12
enter the number
13
enter the number
14
enter the number
15
enter the number
16
enter the number
19
enter the number
47
enter the number
45
enter the number
20
the sum of ten number is211
