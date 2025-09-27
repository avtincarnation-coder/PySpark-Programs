a = "aabccccddeeeee"
str1 = ''
distinct = set(sorted(a))
for i in distinct:
    count=0
    for j in a:
        if i==j:
            count += 1
        else:
            continue
    str1 = str1 + i + str(count)
print(str1)