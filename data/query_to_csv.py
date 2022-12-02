import psycopg2


class TransformQueryCSV:
    def create_csv(self):
        conn = psycopg2.connect(
            host="localhost", database="new_northwind", user="postgres", password="123"
        )
        cur = conn.cursor()

        print("Connecting")

        try:
            select = "select * from orders as ord, order_details as dt where ord.order_id = dt.order_id"
            print(cur.execute(select))
            sql = "COPY (select * from orders as ord, order_details as dt where ord.order_id = dt.order_id) TO STDOUT WITH CSV DELIMITER ','"
            with open("tb_orders_and_order_details.csv", "w") as file:
                cur.copy_expert(sql, file)
                cur.close()
                print("CSV created successfully")
        except:
            print(
                "Error to transform query in csv, please verify check if step two was performed today."
            )

    def run(self):
        try:
            self.create_csv()
            self.log_sql = "No errors found"
        except Exception as errorText:
            self.log_sql = f"{errorText} \n\n Error in step 2 when transform a result of query in CSV !!!"
            self.status_csv = f'{self.tomorrow}'
            with open("data/logfile.txt", "w") as file:
                file.write(self.log_sql)


if __name__ == "__main__":
    step_two = TransformQueryCSV()
    step_two.run()
    print(step_two.log_sql)
