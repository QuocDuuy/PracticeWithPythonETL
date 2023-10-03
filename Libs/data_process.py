import pandas as pd

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

        print(f"Đọc tệp {self.file_name}:")
        return df

# Lớp để xử lý dữ liệu
class dataProcessor:
    def __init__(self, df):
        self.df = df

    def show_info(self):
        print("DataFrame Info:")
        self.df.info()

    def remove_columns(self, columns_to_remove):
        try:
            # Loại bỏ cột được chỉ định (columns_to_remove) từ DataFrame
            self.df.drop(columns=self.df.columns[columns_to_remove - 1], inplace=True)
            return self.df
        except IndexError:
            print("Không tìm thấy cột cần xóa. Hãy đảm bảo bạn đã nhập một số cột hợp lệ.")
            return None

    def rename_columns(self):
        try:
            if isinstance(self.df, pd.DataFrame):
                # Chuyển tên cột thành Proper Case và thay đổi khoảng trắng và dấu gạch ngang thành gạch dưới
                self.df.columns = [col.title().replace(" ", "_").replace("-", "_") for col in self.df.columns]
                print(self.df)
                return self.df
            else:
                print("DataFrame 'self.df' is not available or is not of type pd.DataFrame.")
                return None
        except Exception as e:
            print(f"Failed to rename columns: {str(e)}")
            return None

    def remove_vietnamese_accents(self):
        s1 = u"ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨĩŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴỵỶỷỸỹ"
        s0 = u"AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYyYy"

        def remove_accent_from_string(input_string):
            translation = str.maketrans(s1, s0)
            return input_string.translate(translation)

        if isinstance(self.df, pd.DataFrame):
            # Áp dụng hàm remove_accent_from_string cho tất cả ô trong DataFrame
            self.df = self.df.apply(lambda x: remove_accent_from_string(x) if isinstance(x, str) else x)
            print("Done")
            return self.df
        else:
            print("DataFrame 'self.df' is not available or is not of type pd.DataFrame.")
            return None

    def replace_null_values(self):
        for col in self.df.columns:
            self.df[col].fillna("NA", inplace=True)