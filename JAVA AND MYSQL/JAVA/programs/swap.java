//write a program in java to swap 2 number.
import java.util.Scanner;
public class swap
{
public static void main(String[]args)
{
int num1,num2,temp;
Scanner sc=new Scanner(System.in);
System.out.println("enter the 1st number");
num1=sc.nextInt();
System.out.println("enter the second number");
num2=sc.nextInt();
temp=num1;
num1=num2;
num2=temp;

System.out.println("swapping is of first number is"+num1);
System.out.println("swapping is of second number is"+temp);
}
}





//output:
enter the 1st number
25
enter the second number
55
swapping is of first number is55
swapping is of second number is25
