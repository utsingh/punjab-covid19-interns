import pandas
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from tqdm import tqdm

start=int(input("Start:"))
length=int(input("Length:"))
order=int(input("Order:"))
end=start+length-1
print(f"File will start from {start} and end at {end}")
print(f"For the next iteration, use the value {end+1} as the start value")


def assign_address(df,start,length,order):
    exceptions=[]
    locator = Nominatim(user_agent="myGeocoder")
    geocode = RateLimiter(locator.geocode, min_delay_seconds=0)
    index=df.index
    number_of_rows=len(index)
    for row_num in tqdm(range(start,start+length)):
        row= df.loc[row_num]
        address=str(row["patient_address"])+", "+str(row["patient_block"])+", "+str(row["patient_district_name"])+", Punjab, India"
        location = locator.geocode(address)
        if location==None:
            address=str(row["patient_block"])+", "+str(row["patient_district_name"])+", Punjab, India"
            location = locator.geocode(address)
        if location==None:
            address=str(row["patient_district_name"])+", Punjab, India"
            location = locator.geocode(address) 
        if location==None:
            exceptions.append(row_num+2)
            pass
        else:
            point=tuple(location.point)
            latitude=point[0]
            longitude=point[1]
            df.at[row_num,"final_address"]=address
            df.at[row_num,"latitude"]=latitude
            df.at[row_num,"longitude"]=longitude
            print(location,latitude,longitude)

    exceptions_filename=str(order)+"_exceptions"+".txt"
    with open(exceptions_filename,"w") as f:
        for item in exceptions:
            f.write(f"{item}\n")
    return df



locator = Nominatim(user_agent="myGeocoder")

df=pandas.read_excel("cases.xlsx",8)

df.insert(1,'longitude','')
df.insert(1,'latitude','')
df.insert(1,'final_address','')

df1=df
df1=assign_address(df1,start,length,order)

df1=df1[start:end+1]

output_filename=str(order)+"start"+str(start)+"end"+str(end)+".xlsx"

df1.to_excel(output_filename)



