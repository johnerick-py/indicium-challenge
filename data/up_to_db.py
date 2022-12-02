from sqlalchemy import create_engine, text
from datetime import *
import pandas as pd
import sys


class StepTwoSQL:
    def __init__(self):
        self.final_date = self.date_verification()
        self.tomorrow = self.final_date + timedelta(1)
        self.yesterday = self.final_date - timedelta(1)
        self.engine = create_engine(
            "postgresql://postgres:123@localhost:5432/new_northwind"
        )
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
                "Do you want up directories to database today? Yes = 1, Other date = 0, Stop = 3: "
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
                        f"issue date '{date_input}' issue date is invalid enter a valid date."
                    )

            elif execute_today == "3":
                print("Up to DB is finished")
                sys.exit()
            else:
                print("Enter valid option")

    def send_postgres(self):
        try:
            for k, v in self.table_names.items():
                df = pd.read_csv(rf"./postgres/{v}/{self.final_date}/{v}.csv", sep=",")

                df.to_sql(f"{v}", self.engine)
                print(f"The table {v} has been created successfully")

            df = pd.read_json(
                rf"./csv/order_details/{self.final_date}/order_details.json"
            )
            df.to_sql("order_details", self.engine)
            print(f"The table order_details has been created successfully")

            for v in self.table_names.values():
                with self.engine.connect() as conn:
                    conn.execute(text(f"ALTER TABLE {v} DROP COLUMN index;"))
            with self.engine.connect() as conn:
                conn.execute(text("ALTER TABLE order_details DROP COLUMN index;"))

        except:
            print(
                "The files have already been created on today's date {}".format(
                    self.final_date
                )
            )
            delete_tables = input(
                "Do you want to delete the tables?\n(It is necessary to create new tables on other dates) Yes = 1 No = 0: "
            )
            if delete_tables == "1":
                with self.engine.connect() as conn:
                    for v in self.table_names.values():
                        conn.execute(text(f"DROP TABLE {v}"))
                    conn.execute(text(f"DROP TABLE order_details"))
                print(f"Tables deleted")
                sys.exit()
            elif delete_tables == "0":
                print("Bye")
                sys.exit()
            else:
                print(f"Invalid input")

    def verify_log(self):
        with open("logfile.txt", "r") as file:
            logfile = file.read()
            if str(logfile) == str(self.final_date):
                print(
                    "There was an error in step one, please allow 24 hours to rerun or rerun step one again."
                )
                sys.exit()
            

    def run(self):
        try:
            self.verify_log()
            self.send_postgres()
            self.log_sql = "No errors found"

        except Exception as errorText:
            self.log_sql = (
                f"{errorText} \n\n Error in step 2 when upload to postegreSQL !!!"
            )


if __name__ == "__main__":
    step_two = StepTwoSQL()
    step_two.run()
    print(step_two.log_sql)
