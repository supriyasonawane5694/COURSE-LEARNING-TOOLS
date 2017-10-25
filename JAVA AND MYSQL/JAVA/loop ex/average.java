import java.util.Scanner;
class average
{
public static void main(String[] args)
{
System.out.println("enter 5 no.");
Scanner sc=new Scanner(System.in);
float a,b,c,d,e;
float avg;
a=sc.nextFloat();
b=sc.nextFloat();
c=sc.nextFloat();
d=sc.nextFloat();
e=sc.nextFloat();
avg=(a+b+c+d+e)/5;
System.out.println("average is"+avg);

}
}