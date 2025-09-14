try:
  with open("D:/java/data_new/employees.csv", 'r') as file:
      print(file.readline())
      for x in file:
          print(x)
          if x.isupper():
              print(x)
except FileNotFoundError:
    print("File does not exist")




