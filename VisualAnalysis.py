# Retrieved pyplot sample from: https://matplotlib.org/examples/pie_and_polar_charts/pie_demo_features.html
import matplotlib.pyplot as plt

# First get the file path
filename = raw_input("Please enter the file path: ")

# Open the file
file = open(filename, 'r')

# Get all the lines
lines = file.readlines()

# Use file to populate size
sizes = []

for line in lines:
	if line.startswith('**US RENTER**'):
		# Only add into size of the line starts with '**US RENTER**'
		number = line[13:]
		sizes.append(float(number))

# Pie chart, where the slices will be ordered and plotted counter-clockwise:
labels = ['15 to 24 years', '25 to 34 years', '35 to 44 years', '44 to 54 years', '55 to 64 years', '65 to 74 years', '75 years and over']

# Set color
colors = ['b','g','r','c','m','y','w']

fig1, ax1 = plt.subplots()
ax1.pie(sizes, labels=labels, colors=colors, autopct='%1.2f%%', shadow=False, startangle=90)
ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

# Set title
fig=plt.gcf()
fig.canvas.set_window_title('US Renter Age Distribution')

plt.show()