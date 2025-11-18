import pandas as pd
import numpy as np
from exception import MissingColumnError
from flask import Flask, request, flash, redirect, send_file, render_template, url_for


# Define required columns for each DataFrame
pending_columns_required = ['Plant', 'Sales Order', 'Item','Material No.', 'Sold to', 'Ship to Party Name', 'City','Sch Open Qty.',
                        'UoM','Disp. Date','Route','Incoterms','Inco. Desc.','Destination','Cust. Grp', 'Grp Desc','Trp Zone']

# Define required columns for each DataFrame
#pending_columns_required = ['Plant', 'Sales Order','Material No.', 'Sold to', 'Ship to Party Name', 'Sch Open Qty.','UoM','Disp. Date','Incoterms','Trp Zone']


rate_columns_required = ['Plant', 'Plant Zone', 'Plant Zone Desc', 'CFS Source', 'CFS Destination',
                            'Final Destination', 'Dest. Desc.', 'Route Name', 'MODE', 'Total with STO']
# stock_columns_required = ['Plant', 'Material', 'Closing Stock', 'BUn']
stock_columns_required = ['Plant', 'Material', 'Total Stock(Desp+Tra']
# Total Stock(Desp+Tra


def validate_columns(df, df_name, required_columns):
    """
    Validate if DataFrame `df` contains all `required_columns`.

    Args:
    - df (pd.DataFrame): DataFrame to validate.
    - df_name (str): Name of the DataFrame (for error messages).
    - required_columns (list): List of column names that must be present in `df`.

    Raises:
    - ValueError: If any required column is missing in `df`.
    """
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise   ValueError(f"{df_name} is missing the following required columns: {', '.join(missing_columns)}")


def order_assignment_func(pending_so, rate_file, stock_file):
    '''
    This function is used to get to whome order is assigned from which plant
    customer ordered plant or another plant

    Args:
    pending_so(['excel']) : Pending order file in excel format
    rate_file(['excel']) : Rate file in excel format
    stock_file(['excel']) : Stock file in excel format

    Returns:
    order_with_route_df([DataFrame]) : Dataframe contains details of order place and from which plant placed with price

    orders_root_not_found_df([DataFrame]) : orders_root_not_found_df its a data frame with order not placed
                                            due to not available any another plant and also same plant does not
                                            contain Quentity.
    rate_stck_df([DataFrame]) : It has Total Stock(Desp+Tra with updated quantity.
    '''

    # Read Excel files into DataFrames
    pending_df = pd.read_excel(pending_so)
    rate_df = pd.read_excel(rate_file)
    stock_df = pd.read_excel(stock_file)

    # check if any column not present in uploaded file
    missing_col = [ col for col in pending_columns_required if col not in pending_df.columns]

    if len(missing_col)!=0:
        raise MissingColumnError(missing_col,'Pending Order File')

    # check if any column not present in uploaded file
    missing_col = [ col for col in rate_columns_required if col not in rate_df.columns]

    if len(missing_col)!=0:
        raise MissingColumnError(missing_col,'Rate File')

    # get required columns
    pending_df = pending_df[['Plant', 'Sales Order', 'Item','Material No.', 'Sold to', 'Ship to Party Name', 'City','Sch Open Qty.',
                        'UoM','Disp. Date','Route','Incoterms','Inco. Desc.','Destination','Cust. Grp', 'Grp Desc','Trp Zone']]
    #pending_columns_required = pending_df['Plant', 'Sales Order','Material No.', 'Sold to', 'Ship to Party Name', 'Sch Open Qty.','UoM','Disp. Date','Incoterms','Trp Zone']
    # get necessary columns
    rate_df = rate_df[['Plant', 'Plant Zone', 'Plant Zone Desc', 'CFS Source', 'CFS Destination',
                        'Final Destination', 'Dest. Desc.', 'Route Name', 'MODE', 'Total with STO']]

    # get required columns
    stock_df = stock_df[['Plant', 'Material', 'Total Stock(Desp+Tra']]

    # remove any space left and right available in column
    columns =  [col.strip() for col in list(stock_df.columns)]
    stock_df.columns = columns

    # check if any column not present in uploaded file
    missing_col = [ col for col in stock_columns_required if col not in stock_df.columns]

    if len(missing_col)!=0:
        raise MissingColumnError(missing_col,'Stock File')

    # remove records contains mode = 'OWN'
    pending_df = pending_df[pending_df['Incoterms'] != 'OWN']

    # remove nan value from destination
    pending_df = pending_df[~pending_df['Destination'].isna()]
    pending_df.reset_index(drop=True,inplace=True)


    # add Proposed Level in rate table

    rate_df.sort_values(['Dest. Desc.','Total with STO'],ascending=True,inplace=True)
    rate_df = rate_df[~rate_df.duplicated()]

    # df contains Dest.Desc Na
    rate_df_na = rate_df[rate_df['Dest. Desc.'].isna()]
    rate_df_notna = rate_df[rate_df['Dest. Desc.'].notna()]

    # blow loop provides Destination wise Proposed Level
    proposed_level = {}

    unique_plant_lst = rate_df_notna['Plant'].unique()

    # this loop is used for assignment of Proposed level to destination plant wise asc order
    for j in unique_plant_lst:
        plant_dicti ={}
        plant_wise_df = rate_df_notna[rate_df_notna['Plant'] == j]
        unique_dest_per_plant = plant_wise_df['Dest. Desc.'].unique()
        for i in unique_dest_per_plant:
            plant_dest_wise_df= plant_wise_df[plant_wise_df['Dest. Desc.']== i]
            dicti = {}
            cnt=1
            for k in list(plant_dest_wise_df['Total with STO']):
                if k not in dicti.keys():
                    dicti[k]= 'L'+str(cnt)
                cnt+=1
            plant_dicti[i]= dicti
        proposed_level[j] = plant_dicti

    # assign propsed level
    df_lst = []
    for k,v in proposed_level.items():
        select_plant_df = rate_df_notna[rate_df_notna['Plant']== k]

        for key,val in v.items():
            select_plant_desc_df = select_plant_df[select_plant_df['Dest. Desc.']==key]
            select_plant_desc_df['Proposed Level'] = select_plant_desc_df['Total with STO'].map(val)
            df_lst.append(select_plant_desc_df)
    rate_res_df = pd.concat(df_lst,axis=0)
    # final rate df with added Proposed Level
    rate_df = pd.concat([rate_res_df,rate_df_na],axis=0)
    rate_df.reset_index(drop=True,inplace=True)
    # Merge rate_df and stock_df on 'Plant' column
    rate_stck_df = rate_df.merge(stock_df, on='Plant')
  

    # Prepare data for filtering
    pend_plant = list(pending_df['Plant'])
    pend_mat = list(pending_df['Material No.'])
    # Mode
    pend_inco = list(pending_df['Incoterms'])
    #Dest. Desc.
    pend_dest = list(pending_df['Destination'])
    pend_open_qty = list(pending_df['Sch Open Qty.'])
    pend_Order = list(pending_df['Sales Order'])
    pend_disp_date = list(pending_df['Disp. Date'])
    pend_final_dest = list(pending_df['Trp Zone'])
    pend_sold_to = list(pending_df['Sold to'])
    pend_party_name = list(pending_df['Ship to Party Name'])

    req_info_lst = list(zip(pend_plant, pend_mat, pend_dest, pend_open_qty,pend_inco,pend_Order,pend_disp_date,pend_final_dest,pend_sold_to,pend_party_name))
    # Call filter_funct to process data
    # not_in,lst= filter_funct(rate_stck_df, req_info_lst, pending_df)
    lst= filter_funct(rate_stck_df, req_info_lst, pending_df)
    # print(lst)

    # main_lst = []
    # list_type = []
    # dict_type =[]
    # for i in lst:
    #     if isinstance(i, list):
    #         list_type.append(i)
    #     else:
    #         dict_type.append(i)
    # print(list_type)
    # print(len(dict_type))
    # print('len_lst:',len(list_type))
    # if len(list_type)!=0:
    #     for i in list_type:
    #         if type(i) == list:
    #             main_lst.extend(i)
    #         else:
    #             main_lst.append(i)
    # if len(dict_type)!=0:
    #     main_lst.extend(dict_type)

    # print('main::',len(main_lst))
    order_with_route_df = pd.DataFrame(lst)

    # orders_root_not_found_df = pd.DataFrame(not_in,columns= ['plant', 'material', 'destination', 'open_qty','mode','order_no','disp_date','final_dest','sold_to'])

    return order_with_route_df, rate_stck_df



def filter_funct(rate_stck_df_c1,req_info_lst,pending_df):
    '''This function does calculation for getting result

    Args:
    rate_stck_df_c1([DataFrame]) : Dataframe
    req_info_lst([List]) : List of Tuple
    pending_df([DataFrame]) : Dataframe

    Returns:
    lst : list of Dictionary
    not in : tuple list
    '''
    
    lst = []
    for i in req_info_lst:
        # data dict takes all the result keys and values for each pending order
        data_dict = {}

        # if plant , material , Destination , mode and final destination and Total Stock(Desp+Tra > open qty
        df = rate_stck_df_c1[(rate_stck_df_c1['Plant'] == i[0]) & (rate_stck_df_c1['Material'] == i[1])
                            & (rate_stck_df_c1['Dest. Desc.'] == i[2]) & (rate_stck_df_c1['MODE'] == i[4])
                            & (rate_stck_df_c1['Final Destination'] == i[7])
                            &  (rate_stck_df_c1['Total Stock(Desp+Tra']>= i[3])]
        # when empty dataframe
        if len(df) == 0:

            # if plant , material , Destination , mode and final destination and Total Stock(Desp+Tra> open qty
            df = rate_stck_df_c1[(rate_stck_df_c1['Material'] == i[1]) & (rate_stck_df_c1['Dest. Desc.'] == i[2])
                                & (rate_stck_df_c1['MODE'] == i[4]) & (rate_stck_df_c1['Total Stock(Desp+Tra']>= i[3])
                                & (rate_stck_df_c1['Final Destination'] == i[7])]
            if len(df) ==0:
                # Plant, Material, Destination, Open qty, Incoterm , Salse order, trp zone , sold to makes row unique
                filtered_mat_df = pending_df[(pending_df['Plant'] == i[0]) & (pending_df['Material No.'] == i[1])
                                            & (pending_df['Destination']==i[2]) & (pending_df['Sch Open Qty.'] == i[3])
                                            & (pending_df['Incoterms'] == i[4]) & (pending_df['Sales Order'] == i[5])
                                            & (pending_df['Trp Zone']==i[7]) & (pending_df['Sold to']==i[8]) &
                                            (pending_df['Disp. Date']==i[6])]

                # because in req_info_lst contains duplicate records so that have to select only one records
                filtered_mat_df = filtered_mat_df.iloc[:1,:]

                # filter_df used to check Material available
                filter_df = rate_stck_df_c1[(rate_stck_df_c1['Plant']==i[0]) & (rate_stck_df_c1['Material']==i[1]) & (rate_stck_df_c1['Dest. Desc.']== i[2]) & (rate_stck_df_c1['MODE']== i[4]) & (rate_stck_df_c1['Final Destination'] == i[7])]
                # len(filter_df) == 0 then not able to find out route and cost
                if len(filter_df) == 0:
                    data_dict.update({'order_plant':i[0]})
                    data_dict.update({'confirm':'No (Freight Rate is not Available.)'})
                    data_dict.update({'Required_Stock': i[3]})
                    data_dict.update({'Salse Order' : i[5]})
                    data_dict.update({'Disp. Date': i[6]})
                    data_dict.update({'Trp. Zone':i[7]})
                    data_dict.update({'Customer Name':i[9]})
                    data_dict.update({'Material':i[1]})
                    data_dict.update({'MODE':i[4]})
                    data_dict.update({'Final Destination': i[7]})
                    data_dict.update({'Destination': i[2]})
                    data_dict.update({'Sold to':i[8]})
                    # not_in is a list contains order names of which route is not available in rate_stck_df_c1 dataframe
                    # not_in.append(data_dict)
                
                    lst.append(data_dict)
                    
                    continue

                else:
                    if len(filter_df)>1:
                        filter_df.sort_values('Total with STO',inplace=True)
                        filter_df = filter_df.iloc[:1,:]

                    result_df = filtered_mat_df.merge(filter_df,left_on=['Plant','Incoterms','Destination','Trp Zone','Material No.'],right_on=['Plant','MODE','Dest. Desc.','Final Destination','Material'])
                    result_df = result_df[['Total Stock(Desp+Tra','Plant', 'Plant Zone', 'Plant Zone Desc','Final Destination','Sold to','Dest. Desc.', 'MODE','Total with STO', 'Material','UoM']]
                    result_df.sort_values('Total with STO',inplace=True)
                    data_dict.update({'order_plant':i[0]})
                    data_dict.update({'confirm':'No(Required quantity is less than available quantity.)'})
                    data_dict.update({'Required_Stock': i[3]})
                    data_dict.update({'Salse Order' : i[5]})
                    data_dict.update({'Disp. Date': i[6]})
                    data_dict.update({'Trp. Zone':i[7]})
                    data_dict.update({'Customer Name':i[9]})
                    data_dict.update(result_df.iloc[:1,:].to_dict(orient = 'records')[0])
                   
                    lst.append(data_dict)
                    continue
        # when dataframe > 1
        if len(df) >= 1:
            print('len df =1')
            if len(df[(df['CFS Source'].notna()) & (df['CFS Destination'].notna())])>1:
                df.sort_values('Total with STO',inplace=True)
                # lst_dict contains list of dictionary : to assign Price_category L1, L2, L3...
                lst_dict = df.to_dict(orient='records')

                for j in lst_dict:
                    data_dict.update({'order_plant':i[0]})
                    j.update({'confirm':'Select Route'})
                    j.update({'Required_Stock': i[3]})
                    j.update({'Salse Order' : i[5]})
                    j.update({'Sold to': i[8]})
                    j.update({'Disp. Date':np.nan})
                    j.update({'Customer Name': [9]})
                    j.update({'Trp. Zone':i[7]})
                # multiways_lst.extend(lst_dict)
                print(f"for {i}:",lst_dict)
                print('len of df ==1  lst_dict type is::',len(lst_dict))
                lst.extend(lst_dict)
                continue

            # when CFS Source and CFS Destination is nan then:
            if len(df[(df['CFS Source'].isna()) & (df['CFS Destination'].isna())])>1:
                df = df.sort_values('Total with STO')
                # selecting one record because we have diff value for same root so silecting lower cost
                df = df.iloc[:1,:]

            if i[3]<=df['Total Stock(Desp+Tra'].reset_index(drop=True)[0]:
                closing_stck =  df['Total Stock(Desp+Tra'].reset_index(drop=True)[0] - i[3]
                plant = df['Plant'].reset_index(drop=True)[0]
                total_with_sto = df['Total with STO'].reset_index(drop=True)[0]
                rate_stck_df_c1.loc[(rate_stck_df_c1['Plant'] == plant) & (rate_stck_df_c1['Material'] == i[1])
                                    & (rate_stck_df_c1['Total with STO'] == total_with_sto),'Total Stock(Desp+Tra'] = closing_stck

                data_dict.update({'order_plant':i[0]})
                data_dict.update({'confirm':'Yes'})
                data_dict.update({'Required_Stock': i[3]})
                data_dict.update({'Salse Order' : i[5]})
                data_dict.update({'Sold to': i[8]})
                # add dispatch date
                data_dict.update({'Disp. Date':i[6]})
                data_dict.update({'Trp. Zone':i[7]})
                data_dict.update({'Customer Name': i[9]})
                data_dict.update(df.iloc[:1,:].to_dict(orient = 'records')[0])
                lst.append(data_dict)
    return lst