def df_merge_tables(df, to_merge, left_on, right_on, how):
    return df.merge(
            to_merge, left_on=left_on, right_on=right_on, how=how)