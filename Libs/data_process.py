import pandas as pd
import re
# Lớp để đọc tệp dữ liệu
class fileReader:
    def __init__(self, data_dir, file_name, file_path, file_extension):
        self.data_dir = data_dir
        self.file_name = file_name
        self.file_path = file_path
        self.file_extension = file_extension

    def process_file(self):
        if self.file_extension == ".csv":
            df = pd.read_csv(self.file_path, encoding='utf-8')
        elif self.file_extension == ".xlsx":
            df = pd.read_excel(self.file_path, engine='openpyxl')
        else:
            raise ValueError("Unsupported file type")

        print(f"Đọc tệp {self.file_name} ở {self.file_path}:")
        return df

# Lớp để xử lý dữ liệu
class dataProcessor:
    def __init__(self, df):
        self.df = df
    
    """
    Chức năng "Extract" trong ETL (Extract, Transform, Load) là bước đầu tiên trong quá trình xử lý dữ liệu. Nhiệm vụ chính của bước "Extract" là thu thập dữ liệu từ các nguồn nguyên gốc và đưa chúng vào một định dạng hoặc cấu trúc dữ liệu thích hợp để có thể xử lý và biến đổi dữ liệu trong các bước tiếp theo của quy trình ETL.
    """
    
    def show_info(self):
        print("DataFrame Info:")
        self.df.info()

    def display_basic_info(self, df):
    # Số lượng dòng và cột
        num_rows, num_cols = self.df.shape
        print(f"Số lượng dòng: {num_rows}")
        print(f"Số lượng cột: {num_cols}")

        # Hiển thị một số mẫu dữ liệu
        num_samples = 5  # Số lượng mẫu dữ liệu bạn muốn hiển thị
        print(f"\n5 dữ liệu đầu tiên:")
        print(df.head(num_samples))  # Sử dụng df.head() để hiển thị mẫu dữ liệu


    def change_value_to_datetime(self,df):
        try:
            for cols in self.df.columns:
                if re.search(r'(date|time)', cols, re.IGNORECASE) or pd.api.types.is_datetime64_any_dtype(df[cols]):
                    df[cols] = pd.to_datetime(df[cols])
        except Exception as e:
            print("Can not find datetime type columns")

    """
    Kết thúc phần Extract
    """
    
    """
    Chức năng "Transform" trong quá trình ETL (Extract, Transform, Load) được sử dụng để biến đổi dữ liệu từ định dạng ban đầu của nó thành dạng phù hợp cho mục tiêu cuối cùng
    """
    def remove_columns(self):
        try:
            print("Danh sách các cột:")
            for i, col in enumerate(self.df.columns):
                print(f"{i + 1}. {col}")

            # Nhập số cột để xóa
            min_column = 1
            max_column = len(self.df.columns)
            n = int(input(f"Enter the number of columns to remove ({min_column}-{max_column}): "))

            # Kiểm tra tính hợp lệ của số cột cần xóa
            while n < min_column or n > max_column:
                print(f"Invalid column number. Please enter a number from {min_column} to {max_column}.")
                n = int(input(f"Enter the number of columns to remove ({min_column}-{max_column}): "))

            # Loại bỏ cột được chỉ định (columns_to_remove) từ DataFrame
            self.df.drop(columns=self.df.columns[n - 1], inplace=True)
            return self.df
        except IndexError:
            print("Không tìm thấy cột cần xóa. Hãy đảm bảo bạn đã nhập một số cột hợp lệ.")
            return None


    def rename_columns(self):
        try:
            if isinstance(self.df, pd.DataFrame):
                # Chuyển tên cột thành Proper Case và thay đổi khoảng trắng và dấu gạch ngang thành gạch dưới
                self.df.columns = [col.title().replace(" ", "_").replace("-", "_") for col in self.df.columns]
                # print(self.df)
                return self.df
            else:
                print("DataFrame 'self.df' is not available or is not of type pd.DataFrame.")
                return None
        except Exception as e:
            print(f"Failed to rename columns: {str(e)}")
            return None

    def remove_vietnamese_accents(self):
        try:
            s1 = u"ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨĩŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴỵỶỷỸỹ"
            s0 = u"AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYyYy"

            def remove_accent_from_string(input_string):
                translation = str.maketrans(s1, s0)
                return input_string.translate(translation)

            if isinstance(self.df, pd.DataFrame):
                # Áp dụng hàm remove_accent_from_string cho toàn bộ DataFrame
                self.df = self.df.applymap(lambda x: remove_accent_from_string(x) if isinstance(x, str) else x)
                print("Done")
                return self.df
            else:
                print("DataFrame 'self.df' is not available or is not of type pd.DataFrame.")
                return None
        except Exception as e:
            print(f"Error to remove accent from string: {str(e)}")

    def replace_null_values(self):
        self.df = self.df.fillna("NA")
        return self.df