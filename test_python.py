import pickle

list1 = [1,2,3,4]

with open("data.pkl",'wb') as file:
    pickle.dump(list1,file)

with open("data.pkl",'rb') as file1:
    pickle.load(file1)

print(file1)



