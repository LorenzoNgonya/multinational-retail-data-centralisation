import pandas as pd



sf_sal = pd.read_csv('https://aicore-files.s3.amazonaws.com/Foundations/Data_Formats/Salaries.csv')
sf_sal

import string
alphabet = string.ascii_uppercase
alphabet

index_list = []

for first in alphabet:
    for second in alphabet:
            index_list.append(first + second)

print(index_list)

sf_sal["Id"] = index_list
sf_sal.set_index("Id", inplace=True)
sf_sal
sf_sal.to_csv('data.csv')

sf_sal = pd.read_csv('https://aicore-files.s3.amazonaws.com/Foundations/Data_Formats/Salaries.csv', index_col='Id')
sf_sal

#df = pd.read_csv('data.csv')

type(sf_sal[["BasePay"]])

sf_sal[["BasePay"]]

def find_police(x):
    return "POLICE" in x


# use apply to search for it in JobTitle
sf_sal["isPolice"] = sf_sal["JobTitle"].apply(find_police)

sf_sal[["JobTitle", "isPolice"]]

sf_sal["isPolice"].sum()
# 139 police
sf_sal.describe(include='object')

pd.value_counts(sf_sal["JobTitle"])

sf_sal.groupby(["police"])


#answers 
# 93 unique jobs 
#139 police