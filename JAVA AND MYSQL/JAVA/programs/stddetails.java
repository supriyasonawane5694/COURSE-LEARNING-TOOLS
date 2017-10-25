//write program in java to accept marks of 3 subjects & disply pass if percentage of marks is greater then or equal to 50% otherwise false.
import java.util.Scanner;
public class stddetails
{
public static void main(String[]args)
{
int a,b,c,percentage,sum;
sum=0;
percentage=0;
Scanner sc=new Scanner(System.in);
System.out.println("enter the 1st subject mark");
a=sc.nextInt();

System.out.println("enter the 2nd subject mark");
b=sc.nextInt();
System.out.println("enter the 1st subject mark");
c=sc.nextInt();
sum=a+b+c;
percentage=(sum*100/300);

System.out.println("student percentage is" +percentage);


if(percentage>=50)
{
System.out.println("student is pass");
}
else
{
System.out.println("student is pass");
}
}

}


output:

enter the 1st subject mark
55
enter the 2nd subject mark
56
enter the 1st subject mark
85
student percentage is65
student is pass