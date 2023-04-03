import pandas as pd

def tota_cal(total_df, groupby_col, col_list, tota_cal_df_list):
    tota_cal_df = total_df.groupby([groupby_col]).agg({'max_rating': 'max', 'arap_sum': 'sum', 'accr_sum': 'sum'}).reset_index()
    for each in col_list:
        if each != groupby_col:
            tota_cal_df[each] = 'Total'
    tota_cal_df_list.append(tota_cal_df)


def main():
    # load datasets
    df1 = pd.read_csv('dataset1.csv')
    df2 = pd.read_csv('dataset2.csv')

    # merge datasets
    merged_df = pd.merge(df1, df2, on='counter_party')

    # calculate max rating by counterparty
    max_rating = merged_df.groupby(['legal_entity', 'counter_party', 'tier'])['rating'].max().reset_index()
    max_rating.rename(columns={'rating': 'max_rating'}, inplace=True)

    # calculate sum of value where status = ARAP
    arap_sum = merged_df.loc[merged_df['status'] == 'ARAP'].groupby(['legal_entity', 'counter_party', 'tier'])['value'].sum().reset_index()
    arap_sum.rename(columns={'value': 'arap_sum'}, inplace=True)

    # calculate sum of value where status = ACCR
    accr_sum = merged_df.loc[merged_df['status'] == 'ACCR'].groupby(['legal_entity', 'counter_party', 'tier'])['value'].sum().reset_index()
    accr_sum.rename(columns={'value': 'accr_sum'}, inplace=True)

    # merge the above dataframes to get the final result
    result_df = pd.merge(max_rating, arap_sum, on=['legal_entity', 'counter_party', 'tier'], how='outer')
    result_df = pd.merge(result_df, accr_sum, on=['legal_entity', 'counter_party', 'tier'], how='outer')
    result_df.fillna(0, inplace=True)

    # calculate total for each legal_entity, counter_party, and tier
    total_df = result_df.groupby(['legal_entity', 'counter_party', 'tier']).agg({'max_rating': 'max', 'arap_sum': 'sum', 'accr_sum': 'sum'}).reset_index()

    tota_cal_df_list = [total_df]
    # calculate total for each legal_entity
    col_list = ['legal_entity', 'counter_party', 'tier']

    for each in col_list:
        tota_cal(total_df, each, col_list, tota_cal_df_list)


    # calculate grand total
    grand_total_df = total_df.agg({'max_rating': 'max', 'arap_sum': 'sum', 'accr_sum': 'sum'}).to_frame().T
    grand_total_df['legal_entity'] = 'Total'
    grand_total_df['counter_party'] = 'Total'
    grand_total_df['tier'] = 'Total'
    tota_cal_df_list.append(grand_total_df)

    final_df = total_df.append(tota_cal_df_list).reset_index(drop=True)

    print(final_df)



main()



