import os
from os import path

data_dir = path.join(os.getcwd(), "data")

logs_dir = path.join(data_dir, "logs")
if not path.exists(logs_dir):
    os.mkdir(logs_dir)
