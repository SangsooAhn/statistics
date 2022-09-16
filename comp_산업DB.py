import pandas as pd
from tqdm import tqdm
from typing import Dict, List


def make_vlookup(key: str, value: str, d1: pd.DataFrame, d2: pd.DataFrame) -> Dict[int, str]:
    '''
    2개의 데이터 frame에서 key, value에 해당하는 dict를 반환
    산업 db의 경우 조사표 번호에 해당하는 업체명을 확인하는 용도의 dict를 생성할 수 있음

    '''

    d1_names = d1[[key, value]].set_index(key).to_dict()[value]
    d2_names = d2[[key, value]].set_index(key).to_dict()[value]
    d1_names.update(d2_names)

    return d1_names


def make_comparison_frame(
        d1: pd.DataFrame,
        d2: pd.DataFrame,
        standard: float,
        check_cols: List[str]) -> pd.DataFrame:
    '''
    dataFrame의 check_cols에 대해 standard 이상 차이 내역을 정리하여 반환

    '''

    diff = d2[check_cols] - d1[check_cols]
    diff = diff.fillna(0)

    total = []
    for col in tqdm(check_cols):
        # col = check_cols[0]
        # 전체 합계 차이
        diff_sum = diff[col].sum()
        summary = pd.DataFrame({
            '비교항목': col,
            '차이': diff_sum}, index=[0])
        # 세부 내역

        is_value = diff[col] != 0.0
        detail = diff.loc[is_value, [col]]
        detail = detail.rename(columns={col: '차이'})

        detail['%차이'] = detail['차이'] / diff_sum

        d1_value = d1[col].copy()
        d1_value.name = 'd1'

        d2_value = d2[col].copy()
        d2_value.name = 'd2'

        detail = detail.join(d1_value)
        detail = detail.join(d2_value)
        detail = detail.sort_values(
            by='%차이', key=lambda x: abs(x), ascending=False)
        is_above_standard = abs(detail['%차이']) > standard
        detail = detail.loc[is_above_standard]

        detail.index.name = index_col
        detail = detail.reset_index()
        detail[workplace_name_col] = detail[index_col].map(names)
        detail = detail[[index_col, workplace_name_col,
                         '차이', '%차이', 'd2', 'd1']]
        total.append(pd.concat([summary, detail], axis=1))

    return pd.concat(total, axis=0)

# standard = 0.0
# diff = d2[check_cols] - d1[check_cols]
# diff = diff.fillna(0)

# total = []
# for col in tqdm(check_cols):
#     # col = check_cols[0]
#     # 전체 합계 차이
#     diff_sum = diff[col].sum()
#     summary = pd.DataFrame({
#         '비교항목': col,
#         '차이': diff_sum}, index=[0])
#     # 세부 내역

#     is_value = diff[col] != 0.0
#     detail = diff.loc[is_value, [col]]
#     detail = detail.rename(columns={col: '차이'})

#     detail['%차이'] = detail['차이'] / diff_sum

#     d1_value = d1[col].copy()
#     d1_value.name = 'd1'

#     d2_value = d2[col].copy()
#     d2_value.name = 'd2'

#     detail = detail.join(d1_value)
#     detail = detail.join(d2_value)
#     detail = detail.sort_values(
#         by='%차이', key=lambda x: abs(x), ascending=False)
#     is_above_standard = abs(detail['%차이']) > standard
#     detail = detail.loc[is_above_standard]

#     detail.index.name = index_col
#     detail = detail.reset_index()
#     detail[workplace_name_col] = detail[index_col].map(d1_names)
#     detail = detail[[index_col, workplace_name_col, '차이', '%차이', 'd2', 'd1']]
#     total.append(pd.concat([summary, detail], axis=1))

# pd.concat(total, axis=0).to_excel('total.xlsx')


# # check_cols_n =col.replace('_1', '_수송') for col in check_cols]
# check_cols_n ='기관명'] + check_cols

# # d1 = d1[check_cols].copy()
# # d2 = d2[check_cols].copy()

# # d1.columns = check_cols
# # d2.columns = check_cols


if __name__ == '__main__':

    # d1_filename = '2019_열량(전력1차)_에너지원별(용도전체)_20220225.xlsx'
    # d2_filename = '2020_열량(전력1차)_에너지원별(용도전체)_20220225.xlsx'

    d2_filename = '2020년_열량(전력최종)_에너지원별(용도전체)_20220225.xlsx'
    d1_filename = '2019년_열량(전력최종)_에너지원별(용도전체)_20220225.xlsx'

    d1 = pd.read_excel(d1_filename)
    d2 = pd.read_excel(d2_filename)

    index_col = '조사표번호'
    workplace_name_col = '사업장명'
    year_col = '실적년도'

    names = make_vlookup(index_col, workplace_name_col, d1, d2)

    d1 = d1.set_index(index_col)
    d2 = d2.set_index(index_col)
    d1 = d1.fillna(0)
    d2 = d2.fillna(0)

    full_cols = d1.columns.tolist()
    non_numeric_cols = [year_col, index_col, workplace_name_col]
    check_cols = [col for col in full_cols if not col in non_numeric_cols]

    total_frame = make_comparison_frame(d1, d2, 0.10, check_cols)
    total_frame.to_excel('total_전력최종.xlsx')
    below_300_rank_workplace = d2['합계'].nlargest(300).index.tolist()
    is_below_300_rank_workplace = total_frame[index_col].isin(
        below_300_rank_workplace)
    total_frame.loc[is_below_300_rank_workplace].to_excel(
        'total_sel_전력최종.xlsx')
