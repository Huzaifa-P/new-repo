def df_set_column_type(df, column_name_list, data_type_list):
    return df.astype(dict(zip(column_name_list, data_type_list)))
