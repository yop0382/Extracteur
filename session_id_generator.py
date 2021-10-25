import csv

f = open('session.csv', 'w', newline='\n', encoding='utf-8')
writer = csv.writer(f)

n = 1000000
row=[]

for i in range(1, n):
  data=['SESSION'+str(i)]
  row.append(data)

writer.writerows(row)
f.close()
