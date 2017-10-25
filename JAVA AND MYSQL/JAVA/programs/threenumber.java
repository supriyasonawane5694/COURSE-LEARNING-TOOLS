//write program to accept the 3 number and display maximum number.
import java.util.Scanner;
public class threenumber
{
public static void main(String[]args)
{
int num,max;
max=0;
Scanner sc=new Scanner(System.in);
int i=1;
while(i<=3)
{
System.out.println("enter the number");
num=sc.nextInt();
if(num>max)
{
max=num;
}
i++;
}
System.out.println("the maximum number is"+max);
}
}

output:

enter the number
2
enter the number
5
enter the number
6
the maximum number is6
