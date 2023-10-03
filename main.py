import os
from Libs.data_process import dataProcessor
from Libs.data_process import fileReader

if __name__ == '__main__':
    data_dir = "D:/study/code/python/ETL/data"
    file_list = os.listdir(data_dir)

    print("Please choose a format you want to display: \n1. CSV \n2. Excel \n3. JSON")
    x = input("Enter your selection: ")

    if x == "1" or x == "2":
        if x == "1":
            file_extension = ".csv"
        else:
            file_extension = ".xlsx"

        # Danh sách các tệp có định dạng được chọn
        selected_files = [file_name for file_name in file_list if file_name.endswith(file_extension)]

        for file_name in selected_files:
            file_path = os.path.join(data_dir, file_name)
            file_read = fileReader(data_dir, file_path, file_path, file_extension)
            df = file_read.process_file()

            print("Please choose a feature you want to do: \n1. Extract \n2. Transform \n3. Load")
            x = input("Enter your selection: ")

            if x == "1":
                # Chức năng "Extract"
                print("--->EXTRACT<---")
                dataProcessor(df).show_info()

            elif x == "2":
                # Chức năng "Transform"
                print("--->TRANSFORM<---")
                print("What do you want to do: \n 1. Remove unnecessary columns \n 2. Replace null values")
                x = input("Enter your selection: ")

                if x == "1":
                    # Lựa chọn loại bỏ cột không cần thiết
                    print("Danh sách các cột:")
                    for i, col in enumerate(df.columns):
                        print(f"{i + 1}. {col}")

                    # Nhập số cột để xóa
                    min_column = 1
                    max_column = len(df.columns)
                    n = int(input(f"Enter the number of columns to remove ({min_column}-{max_column}): "))

                    # Kiểm tra tính hợp lệ của số cột cần xóa
                    while n < min_column or n > max_column:
                        print(f"Invalid column number. Please enter a number from {min_column} to {max_column}.")
                        n = int(input(f"Enter the number of columns to remove ({min_column}-{max_column}): "))

                    # Gọi phương thức remove_columns và loại bỏ các cột đã chọn
                    df = dataProcessor(df).remove_columns(n)

                elif x == "2":
                    try:
                        print("Replacing null values...")
                        print("-"*10)
                        # Thực hiện rename columns
                        df = dataProcessor(df).rename_columns()
                        # Thực hiện remove Vietnamese accents
                        df = dataProcessor(df).remove_vietnamese_accents()

                        #replace null values
                        df = dataProcessor(df).replace_null_values()
                    except:
                        print("Unsuccessful to process data")
            elif x == "3":
                # Chức năng "Load" (nếu cần)
                print("--->LOAD<---")
                # Thực hiện chức năng load dữ liệu vào một hệ thống lưu trữ hoặc cơ sở dữ liệu nếu cần

    print("All tasks completed.")
