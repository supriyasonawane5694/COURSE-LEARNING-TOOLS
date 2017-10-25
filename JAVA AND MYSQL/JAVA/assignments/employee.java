import java.util.Scanner;
public class employee
{	
               String empname;
		String empprofile;
		String empmailid;
		
		float empsalary;
		int empage;	
         float basic_sal;

	public employee()
	{
	basic_sal=10000;.
	}
	
		
	
	public void accept()
	{
		

		Scanner sc=new Scanner(System.in);
		System.out.println("enter name of employee");
		empname=sc.nextLine();

		System.out.println("enter profile of employee");
		empprofile=sc.nextLine();

		System.out.println("enter emailid of employee");
		empmailid=sc.nextLine();

		System.out.println("enter salary of employee");
		empsalary=sc.nextFloat();

		System.out.println("enter age of employee");
		empage=sc.nextInt();
		
		System.out.println(empsalary);
        }
		public void check()
               {
                  System.out.println(empprofile);     
		 if(empprofile.equals("manager"))
		{
			float new_empsalary=empsalary+basic_sal;
			System.out.println("manager salary is"+ new_empsalary);
		}
		
	
                }
		
}



 class employee1
{

	public static void main(String[] arg)
	{
		
		
		employee obj=new employee();//invoke construtor
		obj.accept();
		obj.check();
		
		
	}

}