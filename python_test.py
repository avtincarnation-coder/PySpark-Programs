def replacenull(str1, ch):
    if str1 == '':
        print('String is empty')
        return ''

    final_str = ''
    for i in str1:
        if i == ' ':  # replace spaces
            final_str += ch
        else:
            final_str += i
    return final_str


x = 'vf jrs vfs jtjr'
print(replacenull(x, 'a'))
