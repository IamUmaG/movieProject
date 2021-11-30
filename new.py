# Graded Cell, PartID: o1flK

import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    # print(cityToSearch)
    # print(saveLocation1)
    # print(collection)

    data = collection.all()
    businesses = []
    interimresult = []

    # filtering records based on city
    businesses = list(filter(lambda business: business['city'].decode() == cityToSearch, data))

    # formatting the data as needed
    for row in range(len(businesses)):
        interimresult.append([businesses[row]['name'].decode(), businesses[row]['full_address'].decode(),
                              businesses[row]['city'].decode(), businesses[row]['state'].decode()])

        # writing to file
    file = open(saveLocation1, 'w')
    for row_data in interimresult:
        file.write("$".join(str(s) for s in row_data))
        # print("$".join(str(s)  for s in row_data))
        file.write("\n")
    file.close()

def calcDistance(lat2, lon2, lat1, lon1):
    R = 3959
    φ1 = math.radians(lat1)
    φ2 = math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lon2 - lon1)
    a = math.sin(Δφ / 2) * math.sin(Δφ / 2) + math.cos(φ1) * math.cos(φ2) * math.sin(Δλ / 2) * math.sin(Δλ / 2);
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a));
    d = R * c
    return (d)

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    # print(categoriesToSearch)
    # print(myLocation)
    # print(maxDistance)
    # print(saveLocation2)
    # print(collection)

    businessNames = []
    lat1 = myLocation[0]
    lon1 = myLocation[1]

    for i in range(len(collection.all())):
        lat2 = collection.fetch(i).get('latitude')
        lon2 = collection.fetch(i).get('longitude')

        distance = calcDistance(lat2, lon2, lat1, lon1)
        if (distance <= maxDistance):
            categories = collection.fetch(i).get('categories')
        for category in categories:
            if (category.decode() in categoriesToSearch):
                businessName = collection.fetch(i).get('name').decode()
                if (businessName not in businessNames):
                    businessNames.append(businessName)

    # writing to file
    file = open(saveLocation2, 'w')
    for name in businessNames:
        file.write(str(name) + "\n")
        # print(str(name))
    file.truncate(file.tell() - 1)
    file.close()

# FindBusinessBasedOnCity('Tempe','output_city.txt',data)
# FindBusinessBasedOnCity('Scottsdale', 'output_city.txt', data)
# FindBusinessBasedOnCity('Mesa', 'output_city.txt', data)

# FindBusinessBasedOnLocation(['Restaurants'], [33.3482589, -111.9088346], 10, 'output_loc.txt', data)
# FindBusinessBasedOnLocation(['Bakeries'], [33.3482589, -111.9088346], 15, 'output_loc.txt', data)
# FindBusinessBasedOnLocation(['Food', 'Specialty Food'], [33.3482589, -111.9088346], 30, 'output_loc.txt', data)
# FindBusinessBasedOnLocation(['Gardeners'], [33.3482589, -111.9088346], 20, 'output_loc.txt', data)