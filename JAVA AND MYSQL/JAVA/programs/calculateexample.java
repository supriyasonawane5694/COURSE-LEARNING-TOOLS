// write program to perform multiplication,addition,substraction and division function.
import java.util.Scanner;
public class calculateexample
{
public int add(int a, int b)
{
 int sum=a+b;
return sum;
}
public int mul(int a,int b)
{
int result=a*b;
return result;
}
public int sub(int a,int b)
{
int ans =a-b;
return ans;
}
public int div(int a,int b)
{
int dd=a/b;
return dd;
}
public static void main(String[]args)
{

calculateexample ab=new calculateexample();
Scanner sc=new Scanner(System.in);

int p,q,r;

System.out.println("enter the 1st number");
p=sc.nextInt();

System.out.println("enter the 2nd number");
q=sc.nextInt();

r=ab.add(p,q);
System.out.println("addition is "+r);
r=ab.sub(p,q);
System.out.println("substraction is "+r);
r=ab.mul(p,q);
System.out.println("multiplication is "+r);
r=ab.div(p,q);
System.out.println("division is "+r);
}
}


//output:
//enter the 1st number
//50
//enter the 2nd number
//10
//addition is 60
//substraction is 40
//multiplication is 500
//division is 5
