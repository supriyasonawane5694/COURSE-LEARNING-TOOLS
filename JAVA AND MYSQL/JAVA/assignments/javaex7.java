/*WAP in java to create four static function for addition, substraction,multiplication,divide*/
import java.util .Scanner;
public class javaex7
{
public static void add()
{
int a,b,c;
Scanner sc=new Scanner(System.in);
System.out.println("enter no. a=");
a=sc.nextInt();
System.out.println("enter no. b=");
b=sc.nextInt();
c=a+b;
System.out.println("Addition is="+c);
}

public static void multi()
{
int a,b,c;
Scanner sc=new Scanner(System.in);
System.out.println("enter no. a=");
a=sc.nextInt();
System.out.println("enter no. b=");
b=sc.nextInt();
c=a*b;
System.out.println("Multiplication is="+c);
}

public static void sub()
{
int a,b,c;
Scanner sc=new Scanner(System.in);
System.out.println("enter no. a=");
a=sc.nextInt();
System.out.println("enter no. b=");
b=sc.nextInt();
c=a-b;
System.out.println("Subtraction is="+c);
}

public static void divide()
{
int a,b,c;
Scanner sc=new Scanner(System.in);
System.out.println("enter no. a=");
a=sc.nextInt();
System.out.println("enter no. b=");
b=sc.nextInt();
c=a/b;
System.out.println("Division is="+c);
}


public static void main(String[] args)
{
add();
sub();
divide();
multi();
}
}

/*output:
C:\Users\ADMIN\Desktop\supriya>javac javaex7.java

C:\Users\ADMIN\Desktop\supriya>java javaex7
enter no. a=
12
enter no. b=
2
Addition is=14
enter no. a=
23
enter no. b=
20
Subtraction is=3
enter no. a=
20
enter no. b=
5
Division is=4
enter no. a=
34
enter no. b=
2
Multiplication is=68
*/


/*WAP in java to create four nonstatic function for addition, substraction,multiplication,divide
import java.util .Scanner;
public class javaex7
{
public int Add(int a,int b)
{
int sum=a+b;
return sum;
}

public int Sub(int a,int b)
{
int sum=a-b;
return sum;
}

public int Divide(int a,int b)
{
int sum=a/b;
return sum;
}

public int Multi(int a,int b)
{
int sum=a*b;
return sum;
}

public static void main(String[] args)
{
javaex7 obj=new javaex7();
Scanner sc=new Scanner(System.in);
int num1,num2,result;

System.out.println("enter no. num1=");
num1=sc.nextInt();
System.out.println("enter no. num2=");
num2=sc.nextInt();

result=obj.Add(num1,num2);
System.out.println("Addition is="+result);
result=obj.Sub(num1,num2);
System.out.println("Subtraction is="+result);
result=obj.Divide(num1,num2);
System.out.println("Division is="+result);
result=obj.Multi(num1,num2);
System.out.println("Multiplication  is="+result);
}
}

Output:


C:\Users\ADMIN\Desktop\supriya>javac javaex7.java

C:\Users\ADMIN\Desktop\supriya>java javaex7
enter no. num1=
1
enter no. num2=
2
Addition is=3
Subtraction is=-1
Division is=0
Multiplication  is=2
*/



