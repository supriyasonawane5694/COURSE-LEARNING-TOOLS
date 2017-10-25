/*WAP in java to accept product detail like productID,productName,productPrice,productQuantity,& display sum of total amount of to be paid by customer*/
import java.util.Scanner;
public class javaex5
{
	public static void main(String[] args)
	{

	String prodName;
	int prodID,prodPRICE,prodQUANTITY, total;
	Scanner sc=new Scanner(System.in);
	
	System.out.println("Enter product name");
	prodName=sc.next();

	System.out.println("enter product id");
	prodID=sc.nextInt();

	System.out.println("enter product price");
	prodPRICE=sc.nextInt();

	System.out.println("enter product quantity");
	prodQUANTITY=sc.nextInt();

	 total=prodPRICE*prodQUANTITY;
	System.out.println("total"+total);
	
	
	}
}

ans=
C:\Users\ADMIN\Desktop\supriya>java javaex5
Enter product name
s
enter product id
5
enter product price
56
enter product quantity
5
total280
