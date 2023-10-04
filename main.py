import os
from Libs.data_process import dataProcessor
from Libs.data_process import fileReader
from Libs.import_db import connectToDatabase

if __name__ == '__main__':
    try:
        # Kết nối đến cơ sở dữ liệu
        db = connectToDatabase()
        print("Connected to the database")
    except Exception as e:
        print(f"Failed to connect to the database: {str(e)}")

    data_dir = "D:/study/code/python/ETL/PracticeWithPythonETL/data"
    file_list = os.listdir(data_dir)

    selected_files = []

    for file_name in file_list:
        if file_name.endswith(".csv") or file_name.endswith(".xlsx"):
            selected_files.append(file_name)

    try:
        for file_name in selected_files:
            if file_name.endswith(".csv"):
                file_extension = ".csv"
            elif file_name.endswith(".xlsx"):
                file_extension = ".xlsx"

            file_path = os.path.join(data_dir, file_name)
            file_read = fileReader(data_dir, file_path, file_path, file_extension)
            df = file_read.process_file()
            
            print(f"--->EXTRACT for {file_name}<---")
            
            # Hiển thị thông tin cơ bản về dữ liệu
            dataProcessor(df).display_basic_info(df)

            print("--->TRANSFORM<---")
            
            try:
                # Thực hiện rename columns
                df = dataProcessor(df).rename_columns()
                print("Done rename columns")
                print("-"*10)

                # Thực hiện remove Vietnamese accents
                df = dataProcessor(df).remove_vietnamese_accents()
                print("Done remove accents")
                print("-" * 10)

                # replace null values
                df = dataProcessor(df).replace_null_values()
                print("Done replace null values")
                print("-" * 10)

                # Thay đổi kiểu dữ liệu datetime
                dataProcessor(df).change_value_to_datetime(df)




                print("Complete transformation")
            except Exception as e:
                print(f"Unsuccessful to process data: {str(e)}")

            # Chức năng "Load" (nếu cần)
            print("--->LOAD<---")
            # Thực hiện chức năng load dữ liệu vào một hệ thống lưu trữ hoặc cơ sở dữ liệu nếu cần

    except Exception as e:
        print(f"Error processing file: {str(e)}")

    print("All tasks completed.")
