import matplotlib.pyplot as plt
import csv

x = []
y = []
z=[]
with open('C:\\Users\\mkishtapati\\Downloads\\results.csv','r') as csvfile:
    plots = csv.reader(csvfile, delimiter=',')
    for row in plots:
        x.append(row[0])
        y.append(row[2])
        z.append(row[3])
        #b.append(row[3])

plt.plot(x,y, label='Product name & Total Quantity')
plt.xlabel('x')
plt.ylabel('y')
plt.legend()
plt.show()

plt.plot(x,z,label='Product name & Total Sales')
plt.xlabel('x')
plt.ylabel('z')
plt.legend()
plt.show()
