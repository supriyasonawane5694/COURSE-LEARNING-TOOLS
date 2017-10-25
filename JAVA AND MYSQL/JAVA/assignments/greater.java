import java.util.Scanner;
public class greater
{
public static void main(String[] args)
{
int num,max=0;
Scanner sc=new Scanner(System.in);
for(int i=0; i<5;i++)
{
System.out.println("enter a no");
num=sc.nextInt();
if(num>max)
{
max=num;
}
}

System.out.println("maximum no. is"+max);
}
}