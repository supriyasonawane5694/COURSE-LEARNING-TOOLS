/*accept customer detail like customer name,gender, age,address,city,country,pin,phone,area of interest,& DOB
validation=1.customerid must start with'c'and no character must not be more than 4
	   2.customer gender should be male/female
analysis part=1.check no. of male and female
	      2.total no. of customer belong to pune city*/

import java.util.Scanner;
class javaex6
{
	public static void main(String[] args)
	{
	int i=0;
	int custmAge;
	int custmPin;
	int custmPhone;
	int custmDOB;
	int malecount=0;
	int femalecount=0;
	String custmId;
	String custmName;
	String custmGender;
	String custmAddress;
	String custmCity;
	String custmAreaOfInterest;
	int punecount=0;	
	Scanner sc=new Scanner(System.in);
	

	System.out.println("enter the customer Id");
	custmId=sc.nextLine();

	if(custmId.substring(0,4))
	{
		
		System.out.println("customer valid");
	}
	
	if(custmId.startsWith("c"))
	{
		System.out.println("customer valid");
	}
	else
	{
		System.out.println("customer invalid");
	}
	
		

	System.out.println("enter the customer age");
	custmAge=sc.nextInt();

	System.out.println("enter the customer pin");
	custmPin=sc.nextInt();
		
	System.out.println("enter the customer phone no.");
	custmPhone=sc.nextInt();

	System.out.println("enter the customer DOB");	
	custmDOB=sc.nextInt();

	System.out.println("enter the customer name");
	custmName=sc.next();

	System.out.println("enter the customer gender");
	custmGender=sc.next();

	if(custmGender=="male" || custmGender=="female")
		{
		System.out.println("customer is valid");
		}
		else
		{
		System.out.println("customer invalid.");
		}

	System.out.println("enter the customer address");
	custmAddress=sc.next();	

	System.out.println("enter the customer city");
	custmCity=sc.next();

	System.out.println("enter the customer area of interest");
	custmAreaOfInterest=sc.next();
	
	
		

		if(custmGender=="male")
		{
		malecount=malecount+1;
		System.out.println("total male "+malecount);
		}
		else
		{
		femalecount=femalecount+1;
		System.out.println("total female "+femalecount);
		}
		if(custmCity=="pune")
		{
		punecount++;
		}


}
}