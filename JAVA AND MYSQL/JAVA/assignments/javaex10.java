import java.util.Scanner;
class javaex10
{
public static void main(String[] arg)
{
int a, reminder,reverse=0;
Scanner sc=new Scanner(System.in);
System.out.println("enter ano. a=");
a=sc.nextInt();

while(a>0)
{
	reminder=a%10;
	reverse=reverse*10+reminder;
	a=a/10;
}	
System.out.println("reverse number is"+reverse);
}
}

ans=
C:\Users\ADMIN\Desktop\supriya>java javaex10
enter ano. a=
56
reverse number is65
