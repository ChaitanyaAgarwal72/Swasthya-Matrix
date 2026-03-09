import os
import urllib.request

hadoop_bin_dir = r"C:\hadoop\bin"
os.makedirs(hadoop_bin_dir, exist_ok=True)

print("Downloading winutils.exe (Windows translation file for Spark)...")
urllib.request.urlretrieve(
    "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe", 
    os.path.join(hadoop_bin_dir, "winutils.exe")
)

print("Downloading hadoop.dll...")
urllib.request.urlretrieve(
    "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/hadoop.dll", 
    os.path.join(hadoop_bin_dir, "hadoop.dll")
)

print("✅ Windows PySpark patch installed successfully at C:\\hadoop\\bin!")