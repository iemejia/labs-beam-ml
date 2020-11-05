# input = '{ "book":"Test Book", "sentence":"All work and no play makes jack a dull boy, all work and no play. All work and no play makes jack a dull boy, all work and no play. All work and no play makes jack a dull boy, all work and no play."}'
input = 'Africa,Algeria,,Algiers,1,1,1995,64.2'
# so, I passed a record to Python, took one string field there and made it UPPERCASE and then passed it back to Java

element = input.split(',')
output = element[3].upper()
print(output)

