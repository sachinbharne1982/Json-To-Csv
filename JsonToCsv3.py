from copy import deepcopy
import pandas
import json

def cross_join(left, right):
    new_rows = [] if right else left
    for left_row in left:
        for right_row in right:
            temp_row = deepcopy(left_row)
            for key, value in right_row.items():
                temp_row[key] = value
            new_rows.append(deepcopy(temp_row))
    return new_rows

def flatten_list(data):
    for elem in data:
        if isinstance(elem, list):
            yield from flatten_list(elem)
        else:
            yield elem


def json_to_dataframe(data_in):
    def flatten_json(data, prev_heading=''):
        if isinstance(data, dict):
            rows = [{}]
            for key, value in data.items():
                rows = cross_join(rows, flatten_json(value, prev_heading + '.' + key))
        elif isinstance(data, list):
            rows = []
            for i in range(len(data)):
                [rows.append(elem) for elem in flatten_list(flatten_json(data[i], prev_heading))]
        else:
            rows = [{prev_heading[1:]: data}]
        return rows

    return pandas.DataFrame(flatten_json(data_in))
    



if __name__ == '__main__':
    
    #json_data = 
    
    with open('Sample.json') as json_file:
        json_data = json.load(json_file)
          
    df = json_to_dataframe(json_data)
    #print(df)
    
    df.to_csv('Sample.csv')
    df.rename(columns = {'items.item.batters.batter.id':'ID','items.item.type':'Type','items.item.name':'Name',
                     'items.item.batters.batter.type':'Batter',
                     'items.item.topping.type':'Topping'},inplace = True)
    df1 = df[['ID','Type','Name','Batter','Topping']]
    #df1.drop(['Unnamed: 0'],axis=1,inplace =True)
    df1.to_csv('Sample_Final.csv', mode='w')
   
