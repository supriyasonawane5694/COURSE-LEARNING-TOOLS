//write program for reverse number ex=2693 reverse3962.
import java.util.Scanner;
class reverse
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




output:


enter ano. a=
2693
reverse number is3962
