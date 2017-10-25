import java.util.Scanner;
public class add5
{
public static void main(String[] args)
{
int a,total=0;
Scanner sc=new Scanner(System.in);
for(int i=0;i<5;i++)
{
	System.out.println("enterthe number =");
	a=sc.nextInt();
	total=total+a;
}
	System.out.println("sum="+total);
}
}