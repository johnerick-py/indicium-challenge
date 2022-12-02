from connect import *
from datetime import *
import pandas as pd
import os
import sys
import warnings


class StepOneSQL:
    def __init__(self):
        self.final_date = self.date_verification()
        self.tomorrow = self.final_date + timedelta(1)
        self.yesterday = self.final_date - timedelta(1)
        self.table_names = {
            "categories_tb": "categories",
            "customer_customer_demo_tb_tb": "customer_customer_demo",
            "customer_demographics_tb": "customer_demographics",
            "customers_tb": "customers",
            "employee_territories_tb": "employee_territories",
            "employees_tb": "employees",
            "orders_tb": "orders",
            "products_tb": "products",
            "region_tb": "region",
            "shippers_tb": "shippers",
            "suppliers_tb": "suppliers",
            "territories_tb": "territories",
            "us_states_tb": "us_states",
        }

    def date_verification(self):
        while True:

            execute_today = input(
                "Do you want to run conversion pgSQL to CSV today? Yes = 1, Other date = 0, Stop = 3: "
            ).strip()

            if execute_today == "1":
                final_date = date.today()
                return final_date
            elif execute_today == "0":
                date_input = input("Enter the date you want, ex: 2022-11-30:  ").strip()

                try:
                    final_date = datetime.strptime(date_input, "%Y-%m-%d").date()
                    return final_date

                except:
                    print(
                        f"data_emissao '{date_input}' é inválido, por favor digite uma data valida."
                    )
            elif execute_today == "3":
                print("Convert pgSQL to CSV is finished")
                sys.exit()
            else:
                print("Enter valid option")

    def extract_sql(self):
        for v in self.table_names.values():
            directory = f"{v}/{self.final_date}"

            parent_dir = "data/postgres"

            dir_address = os.path.join(parent_dir, directory)

            try:

                if not os.path.exists(directory):

                    os.makedirs(dir_address, 493)
            except FileExistsError:
                print("The directories {}/{} already exists".format(v, self.final_date))

        for v in self.table_names.values():
            csv_exists = f"data/postgres/{v}/{self.final_date}/{v}.csv"

            if os.path.exists(csv_exists):
                print(
                    "The files already exist.. \nThe pipeline has already been executed today!!!"
                )
                break

            query = f""" select * from {v}"""
            csv_name = f"data/postgres/{v}/{self.final_date}/{v}.csv"
            warnings.filterwarnings("ignore")
            sql_query = pd.read_sql_query(query, conn)
            df = pd.DataFrame(sql_query)
            df.to_csv(csv_name, index=False)

    def run(self):
        try:
            self.extract_sql()
            self.log_sql = "No errors found, directories created"
            self.status_sql = ""
        except Exception as errorText:
            self.log_sql = f"{errorText} \n\n Error in step 1 convert sql to csv!!!"
            self.status_sql = f"{self.final_date}"
            with open("data/logfile.txt", "w") as file:
                file.write(self.status_sql)


if __name__ == "__main__":
    step_one = StepOneSQL()
    step_one.run()
    print(step_one.log_sql)
