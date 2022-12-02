from datetime import *
from connect import *
import pandas as pd
import os
import sys


class StepOneCSV():

    def __init__(self):
        self.final_date = self.date_verification()
        self.tomorrow = self.final_date + timedelta(1)
        self.yesterday = self.final_date - timedelta(1)

    def date_verification(self):
        while True:
        
            execute_today = input("Do you want to run conversion CSV to JSON today? Yes = 1, No = 0, Stop = 3: ").strip()
            
            if execute_today == "1":
                final_date = date.today()
                return final_date
            elif execute_today == "0":
                date_input = input("Enter the date you want, ex: 2022-11-30:  ").strip()    
                
                try:
                    final_date = datetime.strptime(date_input, "%Y-%m-%d").date()
       
                    break
                except:
                   print(f"data_emissao '{date_input}' é inválido, por favor digite uma data valida.")
            elif execute_today == "3":
                print('Convert CSV to JSON is finished')
                sys.exit()
            else:
                print('Enter valid option')
    
    
    
    
    
    def extract_csv(self):
        # criando repositorio, se já existir avisar que ja foi criado.
        file = os.path.basename(f'order_details.csv')
        file_name = os.path.splitext(file)[0]
        directory = f'{file_name}/{self.final_date}'
        parent_dir = 'data/csv'
        dir_address = os.path.join(parent_dir, directory)
        #tratativa de erro FileExistsError
        try:
            if not os.path.exists(directory):
                os.makedirs(dir_address, 493)
        except FileExistsError:
            print('The directory {} already exists'.format(dir_address))
                
        
        #transformando csv em json utlizando pandas e salvando dentro de data/csv/...
        csv_file = r'order_details.csv'
        order_df = pd.read_csv(csv_file, sep=',')
        json_output = r'{}/{}.json'.format(dir_address, file_name)
        order_df.to_json(json_output, indent=1)
    
    def run(self):
        
        try:
            self.extract_csv()
            self.log_csv = 'No errors found, directory created successfully'
            self.status_csv = ""
        except Exception as errorText:
            self.log_csv = f'{errorText} \n\n Error in step 1 convert sql to csv!!!'
            self.status_csv = f'{self.tomorrow}'
            with open("data/logfile.txt", "w") as file:
                file.write(self.log_csv)
                
            

if __name__ == '__main__':
    step_one = StepOneCSV()
    step_one.run()
    print(step_one.log_csv)