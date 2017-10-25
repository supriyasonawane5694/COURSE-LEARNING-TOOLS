/*wap to accept student detail like student name,ID,age ,city, country. accept detail of 10 student
validation= 1. student id must me 4character and start with "s"
	     2.student age must not be greater than 100
	     3.country must be india
analysis= 1.display total count of student whose age is>21
	   2.display total count who belong from similar city*/

import java.util.Scanner;
public class javaex9
{
	public static void main(String[] args)
	{
		String stdID;
		String stdNAME;
		String stdCITY;
		String stdCOUNTRY="india";
		int stdAGE;
		int i;
		int count=0;
		int punecount=0;
		int mumbaicount=0;
		int dehlicount=0;
		int kolkatacount=0;
		Scanner sc=new Scanner(System.in);	
	
		for(i=0;i<10;i++)
		{
			System.out.println("enter the student ID");
			stdID=sc.nextLine();
			
				/*if(stdID.lenght() != 4)
				{
					System.out.println("invalid");
					System.out.println("enter correct id");
					stdID=sc.nextLine();
				}
				else{
					System.out.println("valid");
				}
				if(stdID.startsWith("s"))
				{
					System.out.println("valid");
			
				}
				else{
					System.out.println("invalidvalid");
					System.out.println("enter correct id");
					stdID=sc.nextLine();
				}*/
			System.out.println("enter the name of student");
			stdNAME=sc.nextLine();
			
			System.out.println("eneter the student age");
			stdAGE=sc.nextInt();
					
				if(stdAGE>100)
				{
					System.out.println("student not allowed");
					System.out.println("enter correct age");
					stdAGE=sc.nextInt();
				}
				else{
					
					System.out.println("student allowed");
				}
			System.out.println("enter city of student");
			stdCITY=sc.nextLine();
			
			System.out.println("enter the country of student");
			stdCOUNTRY=sc.nextLine();

				if(stdCOUNTRY==india)
				{
					System.out.println("student valid");
					
				}
				else{
					System.out.println("enter country again");
					System.out.println("enter correct country");
					stdCOUNTRY=sc.nextLine();
				}			
				
				
		}
			if(stdAGE>21)
			{
				count=count+1;	
			}
				System.out.println("no. of student with age of 21 are="+count);	

			if(stdCITY==pune)
			{
			punecount=punecount+1;	
			System.out.println("no. of student from pune are="+punecount);	
			}
			elseif(stdCITY==mumbai);{
			punecount=punecount+1;	
			System.out.println("no. of student from mumbai are="+mumbaicount);	
			
			}
			elseif(stdCITY==dehli);{
			dehlicount=dehlicount+1;	
			System.out.println("no. of student from dehli are="+dehlicount);	
			
			}	
			elseif(stdCITY==kolkata);{
			kolkatacount=kolkatacount+1;	
			System.out.println("no. of student from kolkata="+kolkatacount);	
			
			}			
	}
}