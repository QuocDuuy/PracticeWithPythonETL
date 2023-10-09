#import_db.py
import os
from pymongo import MongoClient

def connectToDatabase():
    # Thay thế URL kết nối MongoDB của bạn vào dấu ngoặc kép dưới đây
    client = MongoClient("mongodb://localhost:27017/")  # Thay thế URL của bạn

    # Thay thế tên cơ sở dữ liệu của bạn vào dấu ngoặc kép dưới đây
    database = client["test_etl_stored"]  # Thay thế tên cơ sở dữ liệu của bạn
    return database
