import urllib.request
import os

print("Creating Hadoop directories...")
os.makedirs(r"C:\hadoop\bin", exist_ok=True)

winutils_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe"
hadoop_dll_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/hadoop.dll"

print("Downloading uncorrupted winutils.exe...")
urllib.request.urlretrieve(winutils_url, r"C:\hadoop\bin\winutils.exe")

print("Downloading uncorrupted hadoop.dll...")
urllib.request.urlretrieve(hadoop_dll_url, r"C:\hadoop\bin\hadoop.dll")

print("Download complete. Windows binaries are successfully installed.")