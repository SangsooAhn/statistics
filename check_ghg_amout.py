import pickle
from re import S
import pandas as pd
import numpy as np

from pathlib import Path
from typing import List, Tuple, Any, Dict
from tqdm import tqdm
from dataclasses import dataclass, field
import os

import openpyxl as xl

# 명세서 파일 위치
data_path = Path(r'D:\업무\220109 계수')
# data_path = Path(r'D:\0. 통계분석실(2022)\2. 발전원별 배출계수\명세서_연도전체_기관전체_전체업체_20211014')


BIZ_COL = '사업장 대표업종 코드명'

# 에너지공급(발전, 집단, 복합화력, 연료전지 등) 관련 업종은 총 4개
ALLOWED_BIZ = [
    '증기, 냉ㆍ온수 및 공기 조절 공급업',
    '기타 발전업',
    '화력 발전업',
    # '태양력 발전업', 
    # '원자력 발전업',
    # '수력 발전업',
    # '연료용 가스 제조 및 배관공급업',
    '전기, 가스, 증기 및 공기 조절 공급업']

@dataclass
class SerialInformation:
    filename: str
    key_column: str
    value_column: str


@dataclass
class FrameInfo:

    workplace_no_col:str = '사업장 일련번호'
    enterprise_no_col:str ='관리업체 일련번호'
    equipment_col:str = '배출시설 일련번호'
    parameter_name_col:str = '매개변수명(비정형)'

def pickle_first(filename: str, path: Path = data_path, reload: bool = False, **kwargs) -> Any:
    ''' pickle 파일이 존재할 경우 우선적으로 로드 '''

    filename_pkl = filename[:filename.rfind('.')]+'.pkl'

    if reload:
        data = pd.read_csv(path/filename, encoding='cp949',
                           low_memory=False, **kwargs)
        with open(path/filename_pkl, 'wb') as f:
            pickle.dump(data, f)
        return data

    try:
        with open(path/filename_pkl, 'rb') as f:
            data = pickle.load(f)

    except FileNotFoundError:
        data = pd.read_csv(path/filename, encoding='cp949',
                           low_memory=False, **kwargs)
        with open(path/filename_pkl, 'wb') as f:
            pickle.dump(data, f)

    return data


def check_any_and_must_not_have_keywords(
        series: pd.Series, any_keywords: List[str], must_not_have_keywords: List[str]) -> pd.Series:
    ''' 특정 keyword가 포함되는지 여부를 판단하여 boolean mask series 반환
    any_keywords 리스트 중 한 단어가 포함되면 True
    must_not_have_keywords 리스트 중 단어가 포함되지 않으면 True
     '''

    # 검색 결과 초기화
    result = pd.Series(index=series.index)
    result.loc[:] = False

    if any_keywords:

        for keyword in any_keywords:
            is_included = series.str.contains(keyword, regex=False, na=False)
            result = result | is_included

    if must_not_have_keywords:
            
        for keyword in must_not_have_keywords:
            is_not_included = ~series.str.contains(keyword, regex=False, na=False)
            result = result & is_not_included

    return result


def check_forbidden_keywords(
        series: pd.Series, forbidden_keywords: List[str]) -> pd.Series:
    ''' forbidden 단어가 포함되지 않으면 True
    '''

    # 검색 결과 초기화
    result = pd.Series(index=series.index)
    result.loc[:] = True

    for keyword in forbidden_keywords:
        is_not_included = ~series.str.contains(keyword, regex=False)
        result = result & is_not_included

    return result


def check_isin(
        series: pd.Series, values: List[str]) -> pd.Series:
    ''' 허용되는 단위에 대해서만 True로 반환 '''
    return series.isin(values)

@dataclass
class DataConditions:
    
    # data field
    year:int = None
    # equipment_divs:List[str] = field(default_factory=list)
    # activity_divs:List[str] = field(default_factory=list)
    # activity_names:List[str] = field(default_factory=list)

    workplace_numbers:List[str] = None
    equipment_numbers:List[str] = None
    equipment_divs:List[str] = None
    activity_divs:List[str] = None
    activity_names:List[str] = None


    # column info
    year_col:str = '대상연도'
    workplace_no_col:str = '사업장 일련번호'
    equipment_no_col:str = '배출시설 일련번호'
    equipment_div_col:str = '배출시설 코드명'
    activity_div_col:str = '배출활동분류'
    activity_name_col:str = '배출활동명'

def is_selected(data:pd.DataFrame, conditions:DataConditions)->pd.Series:
    ''' 명세서 데이터에 대해 조건에 해당하는 boolean mask array를 반환'''

    if conditions.year:
        is_selected_year = data[conditions.year_col] == conditions.year
        result = is_selected_year.copy()

    if conditions.workplace_numbers:
        is_selected_workplace = check_isin(series=data[conditions.workplace_no_col], values=conditions.workplace_numbers)
        result = result & is_selected_workplace

    if conditions.equipment_divs:
        is_selected_equipment = check_isin(series=data[conditions.equipment_div_col], values=conditions.equipment_divs)
        result = result & is_selected_equipment

    if conditions.activity_divs:
        is_selected_activity_div = check_isin(series=data[conditions.activity_div_col], values=conditions.activity_divs)
        result = result & is_selected_activity_div

    # 이름의 경우에는 다른 조건과 다르게 OR 조건으로 합침에 유의
    if conditions.activity_names:
        is_selected_activity_name = check_isin(series=data[conditions.activity_name_col], values=conditions.activity_names)

        if conditions.year:
            is_selected_year = data[conditions.year_col] == conditions.year
            result_name = is_selected_activity_name & is_selected_year
        else:
            result_name = is_selected_activity_name

    result = result | result_name

    return result

def is_selected_and(data:pd.DataFrame, conditions:DataConditions)->pd.Series:
    ''' 명세서 데이터에 대해 조건에 해당하는 boolean mask array를 반환'''

    if conditions.year:
        is_selected_year = data[conditions.year_col] == conditions.year
        result = is_selected_year.copy()

    if conditions.workplace_numbers:
        is_selected_workplace = check_isin(series=data[conditions.workplace_no_col], values=conditions.workplace_numbers)
        result = result & is_selected_workplace

    if conditions.equipment_numbers:
        is_selected_workplace = check_isin(series=data[conditions.equipment_no_col], values=conditions.equipment_numbers)
        result = result & is_selected_workplace

    if conditions.equipment_divs:
        is_selected_equipment = check_isin(series=data[conditions.equipment_div_col], values=conditions.equipment_divs)
        result = result & is_selected_equipment

    if conditions.activity_divs:
        is_selected_activity_div = check_isin(series=data[conditions.activity_div_col], values=conditions.activity_divs)
        result = result & is_selected_activity_div

    if conditions.activity_names:
        is_selected_activity_name = check_isin(series=data[conditions.activity_name_col], values=conditions.activity_names)
        result = result & is_selected_activity_name

    return result



# parameter 변수의 unstack을 위한 dataclass
@dataclass
class UnstackInfo:
    unstack_col:str
    equipment_no_col:str ='배출시설 일련번호'
    activity_emission_col:str = '온실가스 배출량(tCO2eq)'
    equipment_emission_col:str = '시설배출량'
    order_col:str = '정렬순서'

    # unstack 시 index에 대한 내용
    common_value_cols:List[str] = field(default_factory=list)   # 배출활동별 값이 동일한 컬럼
    different_value_cols:List[str] = field(default_factory=list)    # 배출활동별 값이 다른 컬럼

    # unstacked 된 결과에 대해 계산 필요 여부에 따라 컬럼을 구분
    # __post_init__에서 result_value_cols 정보를 이용하여 result_index_cols 정보가 설정됨
    result_index_cols:List[str] = field(default_factory=list)   # common_value_cols 중에서 계산이 필요하지 않은 컬럼, 
    result_value_cols:List[str] = field(default_factory=list)   # common_value_cols 중에서 계산이 필요한 컬럼

    # unstack 후 multiindex 처리에 대한 설정
    unstacked_fuel_combustion_selected_cols:List[str] = field(default_factory=list)    
    unstacked_fuel_combustion_selected_rename_cols:List[str] = field(default_factory=list)            
    unstacked_fuelcell_selected_cols:List[str] = field(default_factory=list)
    unstacked_fuelcell_selected_rename_cols:List[str] = field(default_factory=list)        
    
    # 서로 다른 컬럼을 가진 unstacked 결과를 통합하는 dataframe의 컬럼
    output_file_cols:List[str] = field(default_factory=list)        

    def __post_init__(self):
        self.result_index_cols = [
            col for col in self.common_value_cols if col not in self.result_value_cols]

fuel_calorie_unit_pairs = {

    ('ton', 'TJ/Gg'):1/1000,
    ('㎘', 'TJ/1000m³'):1/1000,
    ('천 ㎥', 'TJ/1000000Nm3'):1/1000,
    ('m²', 'MJ/N㎥'):1/1000, # m²은 천 ㎥의 오타, MJ/N㎥는 TJ/1000m³의 오타로 보임 (해당 가정으로 계산된 배출량이 명세서상의 배출량과 유사함)
    ('천 ㎥', 'MJ/N㎥'):1/1000, # MJ/N㎥ 는 'TJ/1000000Nm3'와 동일,
    ('천 ㎥', 'MJ/㎥'):1/1000,
    ('ton', 'MJ/kg'):1/1000,
    ('㎘', 'MJ/kg'):1/1000,
    ('㎘', 'MJ/ℓ'):1/1000, 
    ('㎘', 'TJ/Gg'):1/1000, 
}

CONVERSION_TARGET_MANAGEMENT = {
    
    # 모든 단위를 J단위로 기재
    # 환경부 '사업장 고유배출계수 개발 가이드라인'에 따름
    'J':1,
    'cal':4.1868,
    'toe':1e10*4.1868,
    'Wh':3.6e3

}

def make_short_file():

    filenames = [
        '02.사업장 일반정보_전체업체_2021101400.csv',
        '03.배출시설 현황_전체업체_2021101400.csv',
        '04_1.사업장 총괄 현황_전체업체_2021101400.csv',
        '04_2.바이오매스_전체업체_2021101400.csv',
        '05.배출활동별 배출량_전체업체_2020_2021101400.csv',
        '06.공정별 원단위_전체업체_2021101400.csv',
        '07.에너지 판매 실적_전체업체_2021101400.csv',
    ]

    for filename in filenames:

        filename_short = ''.join(filename.split('.')[:-1] + ['.xlsx'])
        print(filename_short)

        test = pd.read_csv(path/filename, encoding='cp949', nrows=1000)
        test.to_excel(path/filename_short)


def sum_unique_values(
        series: pd.Series) -> float:
    ''' 고유한 값인 경우에는 1번만 더함 '''
    values = pd.Series.unique(series)
    return sum(values)


def tuple_to_list(tuple_data: Tuple[str]) -> List[str]:
    ''' tuple을 list로 변환 '''
    return [string for string in tuple_data]


def make_pivot_table_old():
    ''' 자료의 특정 컬럼에 단어 포함 여부를 이용하여 1차로 자료를 선별하고
    연도, 생산단위의 조건을 부여한 후에 피벗테이블로 결과를 저장
    '''
    filename = '06.공정별 원단위_전체업체_2021101400.csv'

    # selection information

    # contains
    col_to_check = '생산품명'
    forbidden_keywords = ['LNG', 'LPG', '천연가스']

    # is
    year_col = '대상연도'
    target_year = 2020

    # isin
    unit_col = '생산단위'
    allowed_units = ['TJ', 'GJ', 'MJ', 'GKCAL', 'ton', 'MWh', 'kWh']

    biz_col = '사업장 대표업종 코드명'
    allowed_bizs = [
        '증기, 냉ㆍ온수 및 공기 조절 공급업',
        '기타 발전업',
        '화력 발전업',
        '연료용 가스 제조 및 배관공급업',
        '전기 판매업',
        '전기, 가스, 증기 및 공기 조절 공급업']

    # pivot_table
    index = ['대상연도', '사업장 대표업종 코드명', '관리업체명',
             '사업장명', '공정명', '생산품명', '배출활동분류', '생산단위']
    values = ['생산량', '배출량(tCO2eq)', '에너지사용량(TJ)',
              '배출원단위(tCO2eq/생산단위)', '에너지원단위(TJ/생산단위)']
    aggfunc = {
        '생산량': [pd.Series.nunique, sum_unique_values],
        '배출량(tCO2eq)': ['count', np.sum],
        '에너지사용량(TJ)': ['count', np.sum],
        '배출원단위(tCO2eq/생산단위)': [pd.Series.nunique, sum_unique_values],
        '에너지원단위(TJ/생산단위)': [pd.Series.nunique, sum_unique_values]}

    filename_pivot = 'test_pvt.xlsx'
    test = pd.read_csv(path/filename, encoding='cp949')

    is_selected = check_forbidden_keywords(
        series=test[col_to_check], forbidden_keywords=forbidden_keywords)
    is_right_year = test[year_col] == target_year
    is_allowed_unit = check_isin(series=test[unit_col], values=allowed_units)
    is_allowed_biz = check_isin(series=test[biz_col], values=allowed_bizs)

    target_df = test.loc[is_selected & is_right_year &
                         is_allowed_unit & is_allowed_biz].copy()

    target_df_pvt = pd.pivot_table(
        data=target_df, index=index, values=values, aggfunc=aggfunc)

    col_order = aggfunc.keys()
    target_df_pvt = target_df_pvt.reindex(col_order, axis=1, level=0)
    target_df_pvt.to_excel(filename_pivot)


def arrange_intensity_data():
    ''' 자료의 특정 컬럼에 단어 포함 여부를 이용하여 1차로 자료를 선별하고
    연도, 생산단위의 조건을 부여한 후에 피벗테이블로 결과를 저장
    '''
    filename = '06.공정별 원단위_전체업체_2021101400.csv'

    # selection information

    # contains
    col_to_check = '생산품명'
    forbidden_keywords = ['LNG', 'LPG', '천연가스']

    # is
    year_col = '대상연도'
    target_year = 2020

    # isin
    unit_col = '생산단위'
    allowed_units = ['TJ', 'GJ', 'MJ', 'GKCAL', 'ton', 'MWh', 'kWh']

    biz_col = '사업장 대표업종 코드명'
    allowed_bizs = [
        '증기, 냉ㆍ온수 및 공기 조절 공급업',
        '기타 발전업',
        '화력 발전업',
        '연료용 가스 제조 및 배관공급업',
        '전기 판매업',
        '전기, 가스, 증기 및 공기 조절 공급업']

    # groupby
    index = ['대상연도', '사업장 대표업종 코드명', '관리업체명',
             '사업장명', '공정명', '생산품명', '배출활동분류', '생산단위']
    values = ['생산량', '배출량(tCO2eq)', '에너지사용량(TJ)',
              '배출원단위(tCO2eq/생산단위)', '에너지원단위(TJ/생산단위)']
    aggfunc = {
        '생산량': [pd.Series.nunique, sum_unique_values],
        '배출량(tCO2eq)': ['count', np.sum],
        '에너지사용량(TJ)': ['count', np.sum],
        '배출원단위(tCO2eq/생산단위)': [pd.Series.nunique, sum_unique_values],
        '에너지원단위(TJ/생산단위)': [pd.Series.nunique, sum_unique_values]}

    filename_pivot = 'test_pvt.xlsx'

    test = pd.read_csv(data_path/filename, encoding='cp949')

    is_selected = check_forbidden_keywords(
        series=test[col_to_check], forbidden_keywords=forbidden_keywords)
    is_right_year = test[year_col] == target_year
    is_allowed_unit = check_isin(series=test[unit_col], values=allowed_units)
    is_allowed_biz = check_isin(series=test[biz_col], values=allowed_bizs)

    target_df = test.loc[is_selected & is_right_year &
                         is_allowed_unit & is_allowed_biz].copy()

    # target_df_pvt = pd.pivot_table(
    #     data = target_df, index=index, values=values, aggfunc=aggfunc)

    result = []
    for name, frame in target_df.groupby(index)[values]:

        emission = 0
        energy_consumption = 0
        production = 0

        index_dict = {key: value for key, value in zip(index, name)}

        # 생산량(전력, 열) 계산
        for i in range(len(frame)):

            emission += frame['배출량(tCO2eq)'].iloc[i]
            energy_consumption += frame['에너지사용량(TJ)'].iloc[i]

            # 초기값 저장
            if i == 0:
                production += frame['생산량'].iloc[i]
            else:
                # 이전값과 생산량, 배출원단위, 에너지원단위가 동일한 경우에는 생산량을 더하지 않음
                is_emission_different = frame['배출량(tCO2eq)'].iloc[i] != frame['배출량(tCO2eq)'].iloc[i-1]
                is_energy_consumption_different = frame['에너지사용량(TJ)'].iloc[i] != frame[
                    '에너지사용량(TJ)'].iloc[i-1]
                is_production_different = frame['생산량'].iloc[i] != frame['생산량'].iloc[i-1]

                if is_emission_different and is_energy_consumption_different and is_production_different:
                    production += frame['생산량'].iloc[i]

        data = {
            '생산량': production,
            '배출량(tCO2eq)': emission,
            '에너지사용량(TJ)': energy_consumption,
            '개수': len(frame)}
        index_dict.update(data)

        result.append(pd.DataFrame(data=index_dict, index=[0]))

    pd.concat(result).set_index(index).to_excel('test_gp.xlsx')

    #     _pvt = pd.pivot_table(
    #     data = target_df, index=index, values=values, aggfunc=aggfunc)

    # col_order = aggfunc.keys()
    # target_df_pvt = target_df_pvt.reindex(col_order, axis=1, level=0)
    # target_df_pvt.to_excel(filename_pivot)


def arrange_intensity_data_6():
    ''' 자료의 특정 컬럼에 단어 포함 여부를 이용하여 1차로 자료를 선별하고
    연도, 생산단위의 조건을 부여한 후에 피벗테이블로 결과를 저장
    '''
    filename = '06.공정별 원단위_전체업체_2021101400.csv'

    # selection information

    # contains
    col_to_check = '생산품명'
    forbidden_keywords = ['LNG', 'LPG', '천연가스']

    # is
    year_col = '대상연도'
    target_year = 2020

    # isin
    unit_col = '생산단위'
    allowed_units = ['TJ', 'GJ', 'MJ', 'GKCAL', 'ton', 'MWh', 'kWh']

    biz_col = '사업장 대표업종 코드명'
    allowed_bizs = [
        '증기, 냉ㆍ온수 및 공기 조절 공급업',
        '기타 발전업',
        '화력 발전업',
        '연료용 가스 제조 및 배관공급업',
        '전기 판매업',
        '전기, 가스, 증기 및 공기 조절 공급업']

    # groupby
    index = ['대상연도', '사업장 대표업종 코드명', '관리업체명', '사업장 일련번호',
             '사업장명', '공정명', '생산품명', '배출활동분류', '생산단위']
    values = ['생산량', '배출량(tCO2eq)', '에너지사용량(TJ)',
              '배출원단위(tCO2eq/생산단위)', '에너지원단위(TJ/생산단위)']

    test = pd.read_csv(data_path/filename, encoding='cp949')

    is_selected = check_forbidden_keywords(
        series=test[col_to_check], forbidden_keywords=forbidden_keywords)
    is_right_year = test[year_col] == target_year
    is_allowed_unit = check_isin(series=test[unit_col], values=allowed_units)
    is_allowed_biz = check_isin(series=test[biz_col], values=allowed_bizs)

    target_df = test.loc[is_selected & is_right_year &
                         is_allowed_unit & is_allowed_biz].copy()
    # target_df = test.loc[is_selected & is_right_year & is_allowed_unit].copy()

    # debug 용도
    # target_df = target_df.loc[target_df['사업장명'] == '씨엔씨티에너지 주식회사'].copy()

    result = []

    # 배출원별 생산량을 공유하는 경우를 처리
    for name, frame in target_df.groupby(index)[values]:

        emission = 0
        energy_consumption = 0
        production = 0

        index_dict = {key: value for key, value in zip(index, name)}
        nunique_production_value = pd.Series.nunique(frame['생산량'])

        # 생산량(전력, 열) 계산
        for i in range(len(frame)):

            emission += frame['배출량(tCO2eq)'].iloc[i]
            energy_consumption += frame['에너지사용량(TJ)'].iloc[i]

            emission_intensity_in_data = frame['배출원단위(tCO2eq/생산단위)'].iloc[i]
            emission_intensity_calculated = frame['배출량(tCO2eq)'].iloc[i] / \
                frame['생산량'].iloc[i]

            energy_intensity_in_data = frame['에너지원단위(TJ/생산단위)'].iloc[i]
            energy_intensity_calculated = frame['에너지사용량(TJ)'].iloc[i] / \
                frame['생산량'].iloc[i]

            # 초기값 저장
            if i == 0:
                production += frame['생산량'].iloc[i]
            else:
                # 생산량 수치를 공유하고 있는 경우에는 생산량을 더하지 않음
                # 생산량 수치를 공유하고 있는 경우 판단 기준
                # 1)이전값과 현재값의 생산량, 배출원단위, 에너지원단위가 동일
                # 2)자료상의 수치(에너지사용량, 배출량, 생산량)를 통해 계산한 원단위(전력, 배출)가 자료 상의 원단위와 1% 이상 차이
                # 1%는 임의로 정한 값으로 아주 작은 차이를 의미
                # 활동자료별 계산 원단위와 자료 상의 원단위가 많이 차이난다는 것은 원단위를 제품별로 계산했다는 의미임

                # is_emission_different = frame['배출량(tCO2eq)'].iloc[i] != frame['배출량(tCO2eq)'].iloc[i-1]
                # is_energy_consumption_different = frame['에너지사용량(TJ)'].iloc[i] != frame['에너지사용량(TJ)'].iloc[i-1]
                is_production_different = frame['생산량'].iloc[i] != frame['생산량'].iloc[i-1]

                try:
                    is_energy_intensity_same = \
                        (energy_intensity_calculated - energy_intensity_in_data) / \
                        energy_intensity_in_data < 0.01

                    is_emission_intensity_same = \
                        (emission_intensity_calculated - emission_intensity_in_data) / \
                        emission_intensity_in_data < 0.01

                # divide by zero 등 오류 처리
                except:
                    is_energy_intensity_same = is_emission_intensity_same = None

                if is_production_different and is_energy_intensity_same and is_emission_intensity_same:
                    production += frame['생산량'].iloc[i]

        # 생산량 고유값이 유일하면
        if nunique_production_value == 1:
            emission_intensity = emission_intensity_in_data
            energy_intensity = energy_intensity_in_data
        else:
            emission_intensity = emission / production
            energy_intensity = energy_consumption / production

        data = {
            '생산량': production,
            '배출량(tCO2eq)': emission,
            '에너지사용량(TJ)': energy_consumption,
            '개수': len(frame),
            '생산량고유개수': nunique_production_value,
            '배출원단위(tCO2eq/생산단위)': emission_intensity,
            '에너지원단위(TJ/생산단위)': energy_intensity}
        index_dict.update(data)

        result.append(pd.DataFrame(data=index_dict, index=[0]))

    # 1차 정리 결과
    stage1 = pd.concat(result).set_index(index)
    # stage1.to_excel('test_gp1.xlsx')

    # 2차 정리
    # 1차 정리는 공정과 생산품명, 배출활동분류가 동일한 경우의 생산량을 정리한 것으로
    # 공정과 생산품명이 동일하나 배출활동분류가 다른 경우에는 생산량이 중복기재되고 있으므로 이를 제거해야 함

    stage2 = stage1.reset_index()
    index = ['대상연도', '사업장 대표업종 코드명', '관리업체명',
             '사업장 일련번호', '사업장명', '공정명', '생산품명']
    values = ['생산량', '배출량(tCO2eq)', '에너지사용량(TJ)',
              '배출원단위(tCO2eq/생산단위)', '에너지원단위(TJ/생산단위)']
    target_df = stage2.copy()

    result = []

    # index가 1차와 다름
    # 1)배출원단위, 에너지원단위가 동일한 경우에는 중복되는 생산량을 제거
    for name, frame in target_df.groupby(index)[values]:

        # 생산량(전력, 열) 계산
        for i in range(len(frame)):

            emission_intensity_in_data = frame['배출원단위(tCO2eq/생산단위)'].iloc[i]
            energy_intensity_in_data = frame['에너지원단위(TJ/생산단위)'].iloc[i]

            # 첫 행은 아무 처리도 하지 않음
            if i == 0:
                pass
            else:

                is_emission_intensity_same = frame['배출원단위(tCO2eq/생산단위)'].iloc[i] == frame['배출원단위(tCO2eq/생산단위)'].iloc[i-1]
                is_energy_intensity_same = frame['에너지원단위(TJ/생산단위)'].iloc[i] == frame['에너지원단위(TJ/생산단위)'].iloc[i-1]
                # is_production_same = frame['생산량'].iloc[i] == frame['생산량'].iloc[i-1]

                if is_emission_intensity_same and is_energy_intensity_same:
                    frame['생산량'].iloc[i] = np.nan

        result.append(frame)

    stage2 = pd.concat(result).set_index(index)
    stage2.to_excel('test_data6.xlsx')
    return stage2

    #     _pvt = pd.pivot_table(
    #     data = target_df, index=index, values=values, aggfunc=aggfunc)

    # col_order = aggfunc.keys()
    # target_df_pvt = target_df_pvt.reindex(col_order, axis=1, level=0)
    # target_df_pvt.to_excel(filename_pivot)


def arrange_intensity_data_5():
    ''' 자료의 특정 컬럼에 단어 포함 여부를 이용하여 1차로 자료를 선별하고
    연도, 생산단위의 조건을 부여한 후에 피벗테이블로 결과를 저장
    '''
    filename = '05.배출활동별 배출량_전체업체_2020_2021101400.csv'
    test = pd.read_csv(data_path/filename, encoding='cp949')

    # selection information

    # contains
    # col_to_check = '생산품명'
    # forbidden_keywords = ['LNG', 'LPG', '천연가스']

    # is
    year_col = '대상연도'
    target_year = 2020

    # isin
    # unit_col = '생산단위'
    # allowed_units = ['TJ', 'GJ', 'MJ', 'GKCAL', 'ton', 'MWh', 'kWh']

    biz_col = '사업장 대표업종 코드명'
    allowed_bizs = [
        '증기, 냉ㆍ온수 및 공기 조절 공급업',
        '기타 발전업',
        '화력 발전업',
        '연료용 가스 제조 및 배관공급업',
        '전기 판매업',
        '전기, 가스, 증기 및 공기 조절 공급업']

    is_allowed_biz = check_isin(series=test[biz_col], values=allowed_bizs)

    emission_activity_col = '배출활동분류'
    allowed_emission_activity = [
        '고정연소',
        '외부열(스팀)',
        '외부전기'
    ]
    allowed_emission_activity = check_isin(
        series=test[emission_activity_col], values=allowed_emission_activity)

    is_fuel_cell = test['배출활동명'] == '연료전지'

    # groupby
    # index=[
    #     '대상연도', '관장기관', '관리업체 일련번호', '관리업체명', '지정업종',
    #     '법인 대표업종 코드', '법인 대표업종 코드명', '사업장 일련번호', '사업장명',
    #     '사업자등록번호', '사업장 대표업종 코드', '사업장 대표업종 코드명',
    #     '사업장 소재지', '법인중소기업 여부', '소량배출사업장 여부',
    #     '배출시설 일련번호', '배출시설 코드', '배출시설 코드명', '자체 시설명',
    #     '시설규모', '배출활동분류', '배출활동 코드', '배출활동명', '방법론 최소 Tier',
    #     '방법론 적용 Tier', '활동자료 코드', '활동자료 코드명', '활동자료 대분류명',
    #     '활동자료 중분류명', '활동자료명', '활동자료 단위코드', '활동자료 단위코드명']

    # index=[
    #     '배출시설 일련번호', '배출시설 코드명', '자체 시설명',
    #     '활동자료 코드명', '활동자료 중분류명', '활동자료명', '활동자료 단위코드명']

    index = [
        '배출시설 일련번호', '활동자료 코드명', '배출활동명', '활동자료명']

    # values=[
    #     '배출시설 일련번호',
    #     '활동자료 사용량', '매개변수 종류', '매개변수 정형화명칭', '매개변수명(비정형)', '파라미터명',
    #     '정렬순서', '매개변수 단위', '매개변수값', '매개변수적용 Tier', '불확도',
    #     '건습구분', '총량중비중', '온실가스 배출량(tCO2eq)', '시설배출량', '사업장배출량', '관리업체배출량']

    values = [
        '매개변수명(비정형)', '매개변수 단위', '매개변수값', '매개변수적용 Tier', '정렬순서', '활동자료 사용량'
    ]

    # is_selected = check_forbidden_keywords(
    #     series = test[col_to_check], forbidden_keywords = forbidden_keywords)
    is_right_year = test[year_col] == target_year
    # is_allowed_unit = check_isin(series = test[unit_col], values = allowed_units)

    target_df = test.loc[is_right_year & is_allowed_biz &
                         (allowed_emission_activity | is_fuel_cell)].copy()

    # debug 용도
    # target_df = target_df.loc[target_df['배출시설 일련번호'] == 'E1950140001004000001'].copy()

    result = []

    # 공백제거
    target_df['매개변수명(비정형)'] = target_df['매개변수명(비정형)'].str.replace(' ', '')

    # 용어 변경
    target_df.loc[target_df['매개변수명(비정형)'] == 'N2O',
                  '매개변수명(비정형)'] = '온실가스배출계수(N2O)'
    target_df.loc[target_df['매개변수명(비정형)'] == 'CO2',
                  '매개변수명(비정형)'] = '온실가스배출계수(CO2)'
    target_df.loc[target_df['매개변수명(비정형)'] == 'CH4',
                  '매개변수명(비정형)'] = '온실가스배출계수(CH4)'

    # 세로형태로 저장된 데이터를 가로형태로 변환
    for name, frame in tqdm(target_df[index+values].groupby(index)):

        frame.head()
        serial = frame['배출시설 일련번호'].iloc[0]
        frame = frame.set_index(['배출시설 일련번호', '매개변수명(비정형)', '활동자료 코드명'])
        try:

            frame_uns = frame.unstack('매개변수명(비정형)')

        # 중복값 발생
        except ValueError:

            try:

                # '정렬순서' 정보를 이용하여 데이터를 분리
                # 동일한 내용으로 구성되었는지 확인
                items_per_block = frame['정렬순서'].max()
                number_of_blocks = len(frame)/items_per_block

                # 동일한 개수의 블록으로 구성된 경우
                if (number_of_blocks >= 2) and number_of_blocks.is_integer():
                    print(f'serial number = {serial}, 동일 개수 블록 처리')

                    start_indexs = [
                        index*items_per_block for index in range(int(number_of_blocks))]

                    for start_index in start_indexs:
                        sub_frame = frame.iloc[start_index:start_index +
                                               items_per_block]
                        sub_frame_uns = sub_frame.unstack('매개변수명(비정형)')
                        result.append(sub_frame_uns)

                    continue

                frame = frame.reset_index()
                # 동일한 개수의 블록으로 구성되지 않은 경우
                # 중복컬럼 확인
                only_fuel_col_duplicated = pd.Series.unique(
                    frame.loc[frame['매개변수명(비정형)'].duplicated(), '매개변수명(비정형)'])[0] == '연료사용량'

                if only_fuel_col_duplicated:
                    # 활동자료 사용량과 수치가 다른 연료사용량 삭제
                    # print(f'자료 확인 시설일련번호 : {}, 중복회피를 위해 연료사용량 행 중 일부 삭제}')
                    print(f'serial number = {serial}, 연료사용량 중복 기재 처리')
                    is_value_different = frame['활동자료 사용량'] != frame['매개변수값']
                    is_fuel_duplicated = frame['매개변수명(비정형)'].duplicated()
                    frame.drop(
                        frame.loc[is_value_different & is_fuel_duplicated].index, axis=0, inplace=True)

                    frame = frame.set_index(
                        ['배출시설 일련번호', '매개변수명(비정형)', '활동자료 코드명'])
                    frame_uns = frame.unstack('매개변수명(비정형)')
                    frame_uns_swap = frame_uns.swaplevel(0, 1, axis=1)
                    frame_uns_swap = frame_uns_swap.sort_index(axis=1, level=0)

                    result.append(frame_uns_swap)

                    continue

                only_indirect_coeffi_duplicated = pd.Series.unique(
                    frame.loc[frame['매개변수명(비정형)'].duplicated(), '매개변수명(비정형)'])[0] == '간접배출계수'

                # 간접배출계수 중복 처리
                if only_indirect_coeffi_duplicated:
                    print(f'serial number = {serial}, 간접배출계수 중복 처리')
                    for i in range(len(frame)):
                        # i=0
                        if 'N2O' in frame['매개변수 단위'].iloc[i]:
                            frame['매개변수명(비정형)'].iloc[i] = '온실가스배출계수(N2O)'

                        elif 'CH4' in frame['매개변수 단위'].iloc[i]:
                            frame['매개변수명(비정형)'].iloc[i] = '온실가스배출계수(CH4)'

                        elif 'CO2' in frame['매개변수 단위'].iloc[i]:
                            frame['매개변수명(비정형)'].iloc[i] = '온실가스배출계수(CO2)'

                    frame = frame.set_index(
                        ['배출시설 일련번호', '매개변수명(비정형)', '활동자료 코드명'])
                    frame_uns = frame.unstack('매개변수명(비정형)')
                    frame_uns_swap = frame_uns.swaplevel(0, 1, axis=1)
                    frame_uns_swap = frame_uns_swap.sort_index(axis=1, level=0)

                    result.append(frame_uns_swap)

                    # # debug 용도
                    # break
                    continue

                # 예외 처리에 실패한 경우 매개변수 '매개변수 단위' 컬럼을 이용하여 고유하게 변경
                frame = frame.reset_index()
                frame['매개변수명(비정형)'] = frame['매개변수명(비정형)'] + \
                    '_' + frame['매개변수 단위'].fillna('')
                frame = frame.set_index(
                    ['배출시설 일련번호', '매개변수명(비정형)', '활동자료 코드명'])
                frame_uns = frame.unstack('매개변수명(비정형)')

            # 중복값이 발생하면 '정렬순서'를 이용하여 index를 고유하게 수정
            except ValueError:
                frame = frame.reset_index()
                frame['매개변수명(비정형)'] = frame['매개변수명(비정형)'] + \
                    '_' + frame['정렬순서'].astype(str)
                frame = frame.set_index(
                    ['배출시설 일련번호', '매개변수명(비정형)', '활동자료 코드명'])
                frame_uns = frame.unstack('매개변수명(비정형)')

        frame_uns_swap = frame_uns.swaplevel(0, 1, axis=1)
        frame_uns_swap = frame_uns_swap.sort_index(axis=1, level=0)
        result.append(frame_uns_swap)

    # 1차 정리 결과
    stage1 = pd.concat(result)

    # stage1 = pd.read_excel('test_data5.xlsx')
    col_reorder = [
        '연료사용량', '열량계수(순발열량)', '열량계수(총발열량)',
        '온실가스배출계수(CO2)', '온실가스배출계수(N2O)', '온실가스배출계수(CH4)',
        '산화계수', '원료투입량',  '원료별배출계수(CO2)',
        '활동량', '외부에서공급받은열(스팀)사용량',
        '열(스팀)간접배출계수(CO2)', '열(스팀)간접배출계수(N2O)', '열(스팀)간접배출계수(CH4)',
        '외부에서공급받은전력사용량',
        '전력간접배출계수(CO2)', '전력간접배출계수(N2O)', '전력간접배출계수(CH4)',
    ]

    stage1 = stage1.reindex(col_reorder, axis=1, level=0)
    stage1 = stage1.reset_index()
    stage1['사업장 일련번호'] = stage1['배출시설 일련번호'].str[:14]
    # stage1.to_excel('test_data5.xlsx')
    return stage1


def save_dict_to_pickle_in_list(dict_list: List[dict], frame: pd.DataFrame, path: Path = data_path) -> None:
    ''' 목록의 사전 데이터를 pickle 로 저장'''
    for info in dict_list:
        filename, key, value = info.filename, info.key_column, info.value_column
        serial_dict = frame[[key, value]].drop_duplicates().set_index(key).to_dict()[
            value]
        with open(path/filename, 'wb') as f:
            pickle.dump(serial_dict, f)


def load_dict_pickle_in_list(dict_list: List[dict], path: Path = data_path) -> Dict[str, dict]:
    ''' 목록의 dict pickle을 로드 '''
    result = {}
    for info in dict_list:
        filename, dict_name = info.filename, info.filename[:info.filename.rfind(
            '.')]
        with open(path/filename, 'rb') as f:
            result[dict_name] = pickle.load(f)
    return result


def arrange_intensity_data_3():
    ''' 자료의 특정 컬럼에 단어 포함 여부를 이용하여 1차로 자료를 선별하고
    연도, 생산단위의 조건을 부여한 후에 피벗테이블로 결과를 저장
    '''
    filename = '03.배출시설 현황_전체업체_2021101400.csv'
    filename_pkl = filename[:filename.rfind('.')]+'.pkl'

    try:
        with open(data_path/filename_pkl, 'rb') as f:
            data = pickle.load(f)

    except FileNotFoundError:
        data = pd.read_csv(data_path/filename,
                           encoding='cp949', low_memory=False)
        with open(data_path/filename_pkl, 'wb') as f:
            pickle.dump(data, f)

    # selection information
    # is
    year_col = '대상연도'
    target_year = 2020

    biz_col = '사업장 대표업종 코드명'
    allowed_bizs = [
        '증기, 냉ㆍ온수 및 공기 조절 공급업',
        '기타 발전업',
        '화력 발전업',
        '연료용 가스 제조 및 배관공급업',
        '전기 판매업',
        '전기, 가스, 증기 및 공기 조절 공급업']

    is_allowed_biz = check_isin(series=data[biz_col], values=allowed_bizs)

    # 참고용
    all_columns = [
        '대상연도', '관장기관', '관리업체 일련번호', '관리업체명',
        '지정업종', '법인 대표업종 코드', '법인 대표업종 코드명',
        '사업장 일련번호', '사업장명', '사업자등록번호', '사업장 대표업종 코드',
        '사업장 대표업종 코드명', '소량배출 사업장 여부', '배출시설 일련번호',
        '배출시설 코드', '배출시설 코드명', '자체 시설명', '시설 용량', '시설용량 단위',
        '세부시설 용량', '세부시설 용량단위', '일일평균 가동시간(hr/day)',
        '연간 가동일수(day/yr)', '방지시설 대상가스', '방지시설명', '처리효율(%)',
        '배출구(굴뚝) 번호', '소규모시설_UNIT', '신증설 계획', '가동계시예정일',
        '시설규모', 'CO2(ton)', 'CH4(kg)', 'N2O(kg)', 'HFCS(kg)', 'PFCS(kg)',
        'SF6(kg)', 'CO2(tCO2eq)', 'CH4(tCO2eq)', 'N2O(tCO2eq)', 'HFCS(tCO2eq)',
        'PFCS(tCO2eq)', 'SF6(tCO2eq)', 'SCOPE1(tCO2eq)', 'SCOPE2(tCO2eq)',
        '합계(tCO2eq)', '에너지(TJ)', '전력(TJ)', '스팀(TJ)', '합계(TJ)']

    serial_informations = [
        SerialInformation('enterprise_serial.pkl', '관리업체 일련번호', '관리업체명'),
        SerialInformation('site_serial.pkl', '사업장 일련번호', '사업장명'),
        SerialInformation('equipment_serial.pkl', '배출시설 일련번호', '사업장명')
    ]

    # 필요시 저장 함수 호출
    save_dict_to_pickle_in_list(serial_informations, data)
    serial_information_loaded = load_dict_pickle_in_list(serial_informations)

    is_right_year = data[year_col] == target_year
    target_df = data.loc[is_right_year & is_allowed_biz].copy()

    # debug 용도
    # target_df = target_df.loc[target_df['배출시설 일련번호'] == 'E1950140001004000001'].copy()

    target_df.to_excel('test_data3.xlsx')
    return target_df


def arrange_intensity_data_2():
    ''' 자료의 특정 컬럼에 단어 포함 여부를 이용하여 1차로 자료를 선별하고
    연도, 생산단위의 조건을 부여한 후에 피벗테이블로 결과를 저장
    '''
    filename = '02.사업장 일반정보_전체업체_2021101400.csv'
    usecols = [
        '대상연도', '사업장 일련번호', '사업장 명', '사업자 등록번호',
        '사업장 대표업종 코드', '사업장 대표업종 코드명', '사업장 소재지',
        '사업장 주요 생산제품 또는 처리물질', '사업장 연간 생산량 또는 처리량',
        '사업장 상시 종업원수', '사업장 매출액(백만원)', '사업장 에너지 비용(백만원)',
        '사업장 자본금(백만원)', '소량배출 사업장 여부', '할당대상여부', 'CO2(tCO2eq)',
        'CH4(tCO2eq)', 'N2O(tCO2eq)', 'HFCS(tCO2eq)', 'PFCS(tCO2eq)', 'SF6(tCO2eq)',
        'SCOPE1(tCO2eq)', 'SCOPE2(tCO2eq)', '합계(tCO2eq)', '에너지(TJ)', '전력(TJ)',
        '스팀(TJ)', '합계(TJ)', '비고', '절사전 합계(tCO2eq)', '절사전 합계(TJ)']

    data = pickle_first(filename, reload=True, usecols=usecols)

    # selection information
    # is
    year_col = '대상연도'
    target_year = 2020

    biz_col = '사업장 대표업종 코드명'
    allowed_bizs = [
        '증기, 냉ㆍ온수 및 공기 조절 공급업',
        '기타 발전업',
        '화력 발전업',
        '연료용 가스 제조 및 배관공급업',
        '전기 판매업',
        '전기, 가스, 증기 및 공기 조절 공급업']

    is_allowed_biz = check_isin(series=data[biz_col], values=allowed_bizs)
    is_right_year = data[year_col] == target_year

    target_df = data.loc[is_right_year & is_allowed_biz].copy()

    # debug 용도
    # target_df = target_df.loc[target_df['배출시설 일련번호'] == 'E1950140001004000001'].copy()

    target_df.to_excel('test_data2.xlsx')
    return target_df

def arrange_intensity_data_2_all_electricity():
    ''' 사업장 데이터에서 전력을 생산하는 사업장에 대한 정보 추출 '''
    
    filename = '02.사업장 일반정보_전체업체_2021101400.csv'
    usecols = [
        '대상연도', '관리업체명', '사업장 일련번호', '사업장 명', '사업자 등록번호',
        '사업장 대표업종 코드', '사업장 대표업종 코드명', '사업장 소재지',
        '사업장 주요 생산제품 또는 처리물질', '사업장 연간 생산량 또는 처리량',
        '사업장 상시 종업원수', '사업장 매출액(백만원)', '사업장 에너지 비용(백만원)',
        '사업장 자본금(백만원)', '소량배출 사업장 여부', '할당대상여부', 'CO2(tCO2eq)',
        'CH4(tCO2eq)', 'N2O(tCO2eq)', 'HFCS(tCO2eq)', 'PFCS(tCO2eq)', 'SF6(tCO2eq)',
        'SCOPE1(tCO2eq)', 'SCOPE2(tCO2eq)', '합계(tCO2eq)', '에너지(TJ)', '전력(TJ)',
        '스팀(TJ)', '합계(TJ)', '비고', '절사전 합계(tCO2eq)', '절사전 합계(TJ)']

    data = pickle_first(filename, reload=True, usecols=usecols)

    # selection information
    # is
    year_col = '대상연도'
    target_year = 2020

    # biz_col = '사업장 대표업종 코드명'
    # allowed_bizs = [
    #     '증기, 냉ㆍ온수 및 공기 조절 공급업',
    #     '기타 발전업',
    #     '화력 발전업',
    #     '태양력 발전업', 
    #     '원자력 발전업',
    #     '수력 발전업',
    #     '연료용 가스 제조 및 배관공급업',
    #     '전기, 가스, 증기 및 공기 조절 공급업']

    # is_allowed_biz = check_isin(series=data[biz_col], values=allowed_bizs)
    is_right_year = data[year_col] == target_year

    production_col = '사업장 주요 생산제품 또는 처리물질'
    is_generator = check_any_and_must_not_have_keywords(
        data[production_col], 
        any_keywords =['전기', '전력', '열', '온수', '증기', '스팀', ], 
        must_not_have_keywords = ['통신', '버스', '축전지', '모터', '반도체', '케이블', '금', 
        '은', '아연', '전기로', '수도', '수돗물', '발전기', '수송', '사용', '자동차', '판매'])
    
    production_unit_col = '사업장 연간 생산량 또는 처리량'
    allowed_units = ['TJ', 'GJ', 'MJ', 'GKCAL', 'ton', 'MWh', 'kWh']
    is_energy_unit = check_any_and_must_not_have_keywords(
        data[production_col], 
        any_keywords =allowed_units, 
        must_not_have_keywords = ['페쇄'])

    # target_df = data.loc[is_right_year & is_allowed_biz & (is_generator | is_electricity_unit)].copy()
    target_df = data.loc[is_right_year & (is_generator | is_energy_unit)].copy()

    # debug 용도
    # target_df = target_df.loc[target_df['배출시설 일련번호'] == 'E1950140001004000001'].copy()

    target_df.to_excel('data2_generator.xlsx')
    return target_df

def save_pickle(data: Any, filename: str, path: Path) -> None:
    ''' 데이터를 pickle로 저장 '''
    with open(filename, 'wb') as f:
        pickle.dump(data, f)

energy_conversion_table = {

    # joules 단위
    'kWh' : 1e3*860*4.1868,
    'MWh' : 1e6*860*4.1868,
    'GJ' : 1e9,
    'TJ' : 1e12,
    'MJ' : 1e6,
    'GKCAL' : 1e9*4.1868,
    'ton' : 0.6650*1e9*4.1868}

def convert_units(unit_from:str, unit_to:str)->float:
    ''' 단위 환산 '''
    if (energy_conversion_table.get(unit_from, None) is None) or (energy_conversion_table.get(unit_to, None) is None):
        print('energy_conversion_table에 단위가 없음') 
        raise ValueError
    
    return energy_conversion_table[unit_from]/energy_conversion_table[unit_to]

def convert_production_amount_6(row: pd.Series) -> pd.Series:
    ''' 단위에 따라 값을 변환 '''

    # 변환이 필요한 단위
    # 전기는 MWh, 열은 TJ로 변환, 변환이 어려운 ton은 변환하지 않음
    if row['생산품명_변경'] == '전기':

        if row['생산단위'] == 'kWh':
            row['생산량_변환(MWh)'] = row['생산량'] * convert_units('kWh', 'MWh')

        elif row['생산단위'] == 'MWh':
            row['생산량변환(MWh)'] = row['생산량']

    elif row['생산품명_변경'] == '열':

        if row['생산단위'] == 'MWh':
            row['생산량변환(MWh)'] = row['생산량']

        elif row['생산단위'] == 'GJ':
            row['생산량변환(MWh)'] = row['생산량'] * convert_units('GJ', 'MWh')

        elif row['생산단위'] == 'MJ':
            row['생산량변환(MWh)'] = row['생산량'] * convert_units('MJ', 'MWh')

        elif row['생산단위'] == 'GKCAL':
            row['생산량변환(MWh)'] = row['생산량'] * convert_units('GKCAL', 'MWh')

        # 환산이 어려우므로 반영하지 않음
        elif row['생산단위'] == 'ton':
            row['생산량변환(MWh)'] = 0

        elif row['생산단위'] == 'TJ':
            row['생산량변환(MWh)'] = row['생산량'] * convert_units('TJ', 'MWh')

    elif row['생산품명_변경'] == '전기, 열':

        if row['생산단위'] == 'MWh':
            row['생산량변환(MWh)'] = row['생산량']

    return row

def convert_production_units_including_ton(row: pd.Series) -> pd.Series:
    ''' 단위에 따라 값을 변환, 증기의 경우에는 대략적인 환산값이므로 참고용으로만 활용 '''

    # 생산품명에 무관하게 MWh단위로 변경
    # ton의 경우 1ton ≒ 0.6650Gcal 관계로 환산 (온도:190℃일 때, 압력:12.799kg/㎠의 전열을 가정)
    # https://www.i-se.co.kr/mass911

    unit_from = row['생산단위']
    unit_to = 'MWh'
    try:
        row['생산량(MWh)'] = row['생산량'] * convert_units(unit_from, unit_to)
    
    # 단위 환산이 어려운 경우 빈 칸으로 입력
    except:
        row['생산량(MWh)'] = np.nan

    return row


def calc_total_in_data6(data6:pd.DataFrame)->pd.DataFrame:
    ''' 온실가스배출량, 생산량, 에너지사용량 종합 합산값 계산
    1차 가공된 자료의 단위를 변환한 후에 합산하여 반환 '''

    data6.reset_index(inplace=True)

    # '배출활동분류' 로 통합
    # 생산품명 변경
    production_rename = {

        '전기': '전기', '증기': '열', '열': '열', '지역난방순환수(중온수)': '열',
        '전력': '전기', '전기, 열': '전기, 열', '열(온수)': '열', '열, 전기': '전기, 열',
        '스팀': '열', '열및전기': '전기, 열', '열생산': '열', '전기생산': '전기',
        '열 및 전기': '전기, 열', '열(스팀)': '열', '중온수': '열', '지역난방(온수)': '열',
        '열(중온수)': '열', '전기 및 열': '전기, 열', '전기, 증기 생산': '전기, 열',
        '전기, 증기 판매': '전기, 열', 'CHP(열)': '열', 'CHP(전기)': '전기', '#1 PLBwg(열, 영통)': '열',
        '#1 PLBwg(열, 장안)': '열', '#2 PLBwg(열, 장안)': '열', '#2 PLBwg(열, 영통)': '열',
        '#1 PLBs': '열', '#1 PLBw': '열', '#2 PLBs': '열', '#2 PLBw': '열', '우드칩(열)': '열',
        '우드칩(전기)': '전기', '열병합발전시설/온수': '열', '열병합발전시설/전기': '전기',
        '일반보일러(#1 PLBs)/온수': '열', '일반보일러(#2 PLBs)/온수': '열', ' 전기': '전기',
        'Superheated Steam(124KG)': '열', 'High pressure Steam(43KG)': '열', '온수': '열',
        '전기,열': '전기, 열', '전기(송전량)': '전기', '송전량': '전기'
    }
    data6['생산품명_변경'] = data6['생산품명'].map(production_rename)

    # 단위 변경
    # '전기'는 MWh, '열'은 TJ, '전기, 열'은 MWh로 변경
    data6 = data6.apply(lambda row: convert_production_amount_6(row), axis=1)

    # 생산품명 단위로 배출량, 생산량, 에너지사용량 합산
    index = [
        '대상연도', '사업장 대표업종 코드명', '관리업체명', '사업장 일련번호',
        '사업장명']

    columns = ['생산품명_변경']
    values = ['배출량(tCO2eq)', '에너지사용량(TJ)', '생산량변환(MWh)']

    aggfunc = {
        '배출량(tCO2eq)': 'sum',
        '에너지사용량(TJ)': 'sum',
        '생산량변환(MWh)': 'sum'
    }

    grped = pd.pivot_table(data6, index=index, columns=columns, values = values, aggfunc = aggfunc)

    # 배출량, 생산량, 에너지사용량 종합 컬럼 추가
    idx = pd.IndexSlice
    level0 = grped.columns.get_level_values(0).unique()
    for index in level0:
        grped.loc[:, idx[index, '종합']] = grped.loc[:, idx[index, :]].sum(axis=1)
    
    grped.sort_index(axis=1, inplace=True)

    return grped

def load_data(
    save2=False, save3=False, save5=False, save6=False
    # save2=True, save3=True, save5=True, save6=True
    )->Tuple[pd.DataFrame]:
    ''' 각 명세서 파일을 처리하거나, 기존에 처리된 결과를 로드하여 반환 '''
    # 

    # 자료 저장 여부 결정
    # save2, save3, save5, save6 = True, True, True, True
    # save2, save3, save5, save6 = False, False, False, False
    path = Path(os.getcwd())

    filename = 'data2.pkl'
    if save2:
        data2 = arrange_intensity_data_2()
        save_pickle(data2, filename, path)
    data2 = pickle_first(filename, path=path, reload=False)

    filename = 'data3.pkl'
    if save3:
        data3 = arrange_intensity_data_3()
        save_pickle(data3, filename, path)
    data3 = pickle_first(filename, path=path, reload=False)

    filename = 'data5.pkl'
    if save5:
        data5 = arrange_intensity_data_5()
        save_pickle(data5, filename, path)
    data5 = pickle_first(filename, path=path, reload=False)

    filename = 'data6.pkl'
    if save6:
        data6 = arrange_intensity_data_6()
        save_pickle(data6, filename, path)
    data6 = pickle_first(filename, path=path, reload=False)

    return data2, data3, data5, data6


def merge_data():

    path = Path(r'D:\python_dev\ghg')
    
    # 새로 파일을 정리할 필요가 있을 경우 True로 설정
    data2, data3, data5, data6 = load_data(
        save2=False, save3=False, save5=False, save6=False
        # save2=True, save3=True, save5=True, save6=True
    )

    # 5번 데이터 정리
    # 5배출활동(배출시설 일련번호, 사업장 일련번호) + 3사업장(배출시설 일련번호) + 2법인(사업장 일련번호)
    # 추후 가능하면 5번 자료에서 사업장, 법인 정보를 살리는 쪽으로 개선 필요

    data6_total = calc_total_in_data6(data6)

    # 사업장 정보 통합
    key = ['사업장 일련번호']
    primary_info = ['사업장 주요 생산제품 또는 처리물질', '사업장 연간 생산량 또는 처리량', '합계(tCO2eq)', '합계(TJ)']
    data2_select = data2[key + primary_info].copy()
    columns_rename = {
        '합계(tCO2eq)':'사업장 배출량(tCO2eq)',
        '합계(TJ)':'에너지사용량(TJ)'
    }
    data2_select.rename(columns=columns_rename, inplace=True)
    data2_select.set_index(key, inplace=True)
    data2_select.columns = pd.MultiIndex.from_product([['02사업장'], data2_select.columns])
    data6_total = data6_total.join(data2_select)

    return data6_total

def load_pre_review_result():

    filename = '배출량 취합v2.0.xlsx'
    sheet_name = 'Sheet1'
    usecols = [
        '구분1', '구분2', '사업장', '사업장 일련번호', '발전량(한전)', '배출량(한전)', 
        '배출계수(한전)', '발전량(명세서)', '배출량(명세서)', '배출계수(명세서)']
    key = '사업장 일련번호'
    level0_col = '기존결과'

    pre_review_result = pd.read_excel(filename, sheet_name=sheet_name, skiprows=1)

    
    pre_review_result = pre_review_result[usecols].copy()
    drop_index = pre_review_result.loc[pre_review_result[key].isna()].index
    pre_review_result.drop(drop_index, axis=0, inplace=True)
    # result_pre.to_excel('result_pre.xlsx')

    pre_review_result.set_index([key], inplace=True)
    pre_review_result.columns = pd.MultiIndex.from_product([[level0_col], pre_review_result.columns])
    pre_review_result_index_duplicated = pre_review_result.index.duplicated()
    pre_review_result.drop(pre_review_result.loc[pre_review_result_index_duplicated].index, inplace=True)

    return pre_review_result

def load_kepco_data():

    save_kepco = False
    if save_kepco:
        filename = '21년도판 한국전력통계(제90호).xlsx'
        sheet_name = '3.소별발전'
        kepco_data = pd.read_excel(filename, sheet_name= sheet_name, skiprows=2)
        save_pickle(kepco_data, 'kepco_generation.pkl', Path(os.getcwd()))

    kepco_data = pickle_first('kepco_generation.pkl', Path(os.getcwd()))
    return kepco_data    



def arrange_all_generation_equipment_data_3():
    ''' 발전관련 모든 시설을 분석
    '''
    filename = '03.배출시설 현황_전체업체_2021101400.csv'
    filename_pkl = filename[:filename.rfind('.')]+'.pkl'

    try:
        with open(data_path/filename_pkl, 'rb') as f:
            data = pickle.load(f)

    except FileNotFoundError:
        data = pd.read_csv(data_path/filename,
                           encoding='cp949', low_memory=False)
        with open(data_path/filename_pkl, 'wb') as f:
            pickle.dump(data, f)

    # selection information
    # is
    year_col = '대상연도'
    target_year = 2020

    is_allowed_biz = check_isin(series=data[BIZ_COL], values=ALLOWED_BIZ)

    # # 참고용
    # all_columns = [
    #     '대상연도', '관장기관', '관리업체 일련번호', '관리업체명',
    #     '지정업종', '법인 대표업종 코드', '법인 대표업종 코드명',
    #     '사업장 일련번호', '사업장명', '사업자등록번호', '사업장 대표업종 코드',
    #     '사업장 대표업종 코드명', '소량배출 사업장 여부', '배출시설 일련번호',
    #     '배출시설 코드', '배출시설 코드명', '자체 시설명', '시설 용량', '시설용량 단위',
    #     '세부시설 용량', '세부시설 용량단위', '일일평균 가동시간(hr/day)',
    #     '연간 가동일수(day/yr)', '방지시설 대상가스', '방지시설명', '처리효율(%)',
    #     '배출구(굴뚝) 번호', '소규모시설_UNIT', '신증설 계획', '가동계시예정일',
    #     '시설규모', 'CO2(ton)', 'CH4(kg)', 'N2O(kg)', 'HFCS(kg)', 'PFCS(kg)',
    #     'SF6(kg)', 'CO2(tCO2eq)', 'CH4(tCO2eq)', 'N2O(tCO2eq)', 'HFCS(tCO2eq)',
    #     'PFCS(tCO2eq)', 'SF6(tCO2eq)', 'SCOPE1(tCO2eq)', 'SCOPE2(tCO2eq)',
    #     '합계(tCO2eq)', '에너지(TJ)', '전력(TJ)', '스팀(TJ)', '합계(TJ)']

    
    
    # serial_informations = [
    #     SerialInformation('enterprise_serial.pkl', '관리업체 일련번호', '관리업체명'),
    #     SerialInformation('site_serial.pkl', '사업장 일련번호', '사업장명'),
    #     SerialInformation('equipment_serial.pkl', '배출시설 일련번호', '사업장명')
    # ]

    # # 필요시 저장 함수 호출
    # save_dict_to_pickle_in_list(serial_informations, data)
    # serial_information_loaded = load_dict_pickle_in_list(serial_informations)

    is_right_year = data[year_col] == target_year
    target_df = data.loc[is_right_year & is_allowed_biz].copy()

    # debug 용도
    # target_df = target_df.loc[target_df['배출시설 일련번호'] == 'E1950140001004000001'].copy()

    target_df.to_excel('generation_equipment_data3.xlsx')
    return target_df



def arrange_generation_related_biz_in_data_6():
    ''' 생산품명의 생산단위 데이터를 이용하여 발전 관련 업종 선별 
    '''
    filename = '06.공정별 원단위_전체업체_2021101400.csv'
    test = pd.read_csv(data_path/filename, encoding='cp949')

    # selection information
    # contains
    col_to_check = '생산품명'
    forbidden_keywords = ['LNG', 'LPG', '천연가스']

    # is
    year_col = '대상연도'
    target_year = 2020

    # isin
    unit_col = '생산단위'
    allowed_units = ['TJ', 'GJ', 'MJ', 'GKCAL', 'ton', 'MWh', 'kWh']

    # biz_col = '사업장 대표업종 코드명'
    # allowed_bizs = [
    #     '증기, 냉ㆍ온수 및 공기 조절 공급업',
    #     '기타 발전업',
    #     '화력 발전업',
    #     '연료용 가스 제조 및 배관공급업',
    #     '전기 판매업',
    #     '전기, 가스, 증기 및 공기 조절 공급업']

    is_selected = check_forbidden_keywords(
        series=test[col_to_check], forbidden_keywords=forbidden_keywords)

    is_right_year = test[year_col] == target_year
    is_allowed_unit = check_isin(series=test[unit_col], values=allowed_units)
    # is_allowed_biz = check_isin(series=test[biz_col], values=allowed_bizs)

    # target_df = test.loc[is_selected & is_right_year & is_allowed_unit & is_allowed_biz].copy()
    target_df = test.loc[is_selected & is_right_year & is_allowed_unit].copy()
    target_df.to_excel('target_df_generation_biz.xlsx')
    # target_df = test.loc[is_selected & is_right_year & is_allowed_unit].copy()

    # debug 용도
    # target_df = target_df.loc[target_df['사업장명'] == '씨엔씨티에너지 주식회사'].copy()

    return target_df

def sum_energy_generation_emission_in_data_5():
    ''' 에너지공급 시설에 해당하는 시설에 대해 배출량 자료 선별
    '''
    filename = '05.배출활동별 배출량_전체업체_2020_2021101400.csv'
    data = pd.read_csv(data_path/filename, encoding='cp949')

    is_allowed_biz = check_isin(series=data[BIZ_COL], values=ALLOWED_BIZ)

    emission_activity_col = '배출활동분류'
    allowed_emission_activity = ['고정연소', '기타']
    is_allowed_emission_activity = check_isin(
        series=data[emission_activity_col].str.strip(), values=allowed_emission_activity)

    emission_activity_name_col = '배출활동명'
    allowed_emission_activity_name = ['고체연료연소', '기체연료연소', '액체연료연소', '연료전지']
    is_allowed_emission_activity_name = check_isin(
        series=data[emission_activity_name_col].str.strip(), values=allowed_emission_activity_name)

    equipment_col = '배출시설 코드명'
    allowed_equipment = [
        '열병합 발전시설', '화력 발전시설', '연료전지', '소각보일러', '발전용 내연기관']
    is_allowed_equipment = check_isin(series=data[equipment_col].str.strip(), values=allowed_equipment)

    is_etc_fuelcell = check_isin(series=data[equipment_col].str.strip(), values=['개질공정'])
    is_etc_fuelcell = is_etc_fuelcell & check_any_and_must_not_have_keywords(data['자체 시설명'], ['연료전지'], None)

    # 열공급 관련 업종의 보일러 설비 반영
    is_peak_load_boiler = check_isin(series=data[equipment_col].str.strip(), values=['일반 보일러시설'])
    is_peak_load_boiler = is_peak_load_boiler & check_isin(
        series=data[BIZ_COL].str.strip(), values=['증기, 냉ㆍ온수 및 공기 조절 공급업'])

    # 기타 SRF 보일러 추가
    is_etc_boiler = check_isin(series=data[equipment_col].str.strip(), values=['기타'])
    is_etc_boiler = is_etc_boiler & check_isin(series=data[BIZ_COL].str.strip(), values=['증기, 냉ㆍ온수 및 공기 조절 공급업'])
    is_etc_boiler = is_etc_boiler & check_any_and_must_not_have_keywords(data['자체 시설명'], ['보일러'], None)
   
    is_data_selected = (
        is_allowed_emission_activity & is_allowed_emission_activity_name & 
        is_allowed_equipment | is_etc_fuelcell | is_etc_boiler | is_peak_load_boiler)

    target_df = data.loc[is_allowed_biz & is_data_selected].copy()
    target_df = target_df.to_excel('5번 파일 정리자료.xlsx')

    activity_amount_col = '활동자료 사용량'
    parameter_value_col = '매개변수값'

    is_activity_value_same = data[activity_amount_col] == data[parameter_value_col]
    emission_df = data.loc[is_allowed_biz & is_data_selected & is_activity_value_same].copy()
    index = [
        '지정업종', '관리업체명', '사업장 일련번호', '사업장명', '사업장 대표업종 코드명', '배출시설 일련번호', 
        '배출시설 코드명', '자체 시설명', '배출활동분류', '배출활동명', '활동자료 코드명']
    values = ['온실가스 배출량(tCO2eq)']

    emission = emission_df.groupby(index)[values].sum()
    emission.to_excel('5번 파일 배출량 내역1.xlsx')


    index = ['지정업종', '관리업체명', '사업장 일련번호', '사업장명', '사업장 대표업종 코드명']
    values = ['온실가스 배출량(tCO2eq)']

    emission = emission_df.groupby(index)[values].sum()
    emission.to_excel('5번 파일 배출량 내역2.xlsx')

    emission_workplace_no_col = '사업장 일련번호'
    emission = emission.reset_index()
    workplace_list = emission[emission_workplace_no_col].unique().tolist()
    save_pickle(workplace_list, 'energy_generation_workplace_list.pkl', Path(os.getcwd()))

    return target_df


def extract_workplace_main_product():
    ''' 2번 파일에서 사업장의 주요 생산품을 확인 '''
    workplace_list = pickle_first('energy_generation_workplace_list.pkl', Path(os.getcwd()))
    
    filename = '02.사업장 일반정보_전체업체_2021101400.csv'
    data2 = pickle_first(filename)
    is_energy_generation_workplace = data2['사업장 일련번호'].isin(workplace_list)

    year_col = '대상연도'
    target_year = 2020
    is_right_year = data2[year_col] == target_year

    is_selected = is_right_year & is_energy_generation_workplace
    
    values = [
        '관리업체명', '사업장 일련번호', '사업장 명', '사업장 소재지', 
        '사업장 주요 생산제품 또는 처리물질', '사업장 연간 생산량 또는 처리량', '합계(tCO2eq)', 
        '에너지(TJ)', '전력(TJ)', '스팀(TJ)', '합계(TJ)']

    main_product = data2.loc[is_selected, values].copy()
    main_product.to_excel('사업장 선택결과.xlsx')

    products = main_product['사업장 주요 생산제품 또는 처리물질'].str.split('|')
    product_amounts = main_product['사업장 연간 생산량 또는 처리량'].str.split('|')

    term_rename = {
        # '전기':'전기', '열':'열', '증기':'증기', '전력 및 스팀' : '전기 및 증기', '외부수열':'외부수열',
        '송전량':'전기', '스팀':'증기', '에너지':'전기 및 열', '열 및 전기':'전기 및 열', 
        '열 및 전기 생산량':'전기 및 열', '열(생산)':'열', '열(수열)':'외부수열', '열(스팀)':'증기', 
        '열(온수)':'열', '열(외부수열)':'외부수열', '열(자체생산)':'열', '열, 전기':'전기 및 열', 
        '열및전기':'전기 및 열', '열생산량':'열', '온수':'열', '전기(태양광발전전력)':'전기(태양광)', '전기,열' : '전기 및 열',
        '전기(화력발전전력)':'전기', '전기, 온수':'전기 및 열', '전기생산량':'전기', '전력':'전기', 
        '지역난방순환수(중온수)':'열', '지역난방열':'열', '태양광':'전기(태양광)', 
        '증기, 전기, 온수':'전기 및 열', '전기, 열':'전기 및 열'}

    max_production_length = max([len(product) for product in products])
    new_columns = ['item'+str(i) for i in range(max_production_length)]

    # 빈 프레임 만들기
    products_frame = pd.DataFrame(index = products.index, columns = new_columns)

    for i, product in enumerate(products):
        for j, item in enumerate(product):
            products_frame.iloc[i, j] = item
    
    products_frame = products_frame.applymap(lambda x:term_rename.get(x, x))
    
    new_columns = ['value'+str(i) for i in range(max_production_length)]
    product_amounts_frame = pd.DataFrame(index = products.index, columns = new_columns)

    for i, product in enumerate(product_amounts):
        for j, item in enumerate(product):
            product_amounts_frame.iloc[i, j] = item

    total = pd.concat([products_frame, product_amounts_frame], axis=1)
    total.to_excel('product_total_1차.xlsx')
    
    categories = set(products_frame.values.flatten())
    
    columns_list = list(categories)
    new_total = pd.DataFrame(index=total.index, columns = columns_list)
    for i in range(len(products_frame)):
        for j in range(len(products_frame.columns)):
            column_position = columns_list.index(products_frame.iloc[i, j])
            new_total.iloc[i, column_position] = product_amounts_frame.iloc[i, j]

    new_total.to_excel('new_total.xlsx')
    return new_total


def extrace_workplace_process_information():
    ''' 6번 파일에서 사업장의 공정 정보를 확인 '''
    
    filename = '06.공정별 원단위_전체업체_2021101400.csv'
    data = pickle_first(filename)

    year_col = '대상연도'
    target_year = 2020
    is_right_year = data[year_col] == target_year

    workplace_list = pickle_first('energy_generation_workplace_list.pkl', Path(os.getcwd()))
    is_energy_generation_workplace = data['사업장 일련번호'].isin(workplace_list)

    emission_activity_col = '배출활동분류'
    allowed_emission_activity = ['고정연소', '기타']
    is_allowed_emission_activity = check_isin(
        series=data[emission_activity_col].str.strip(), values=allowed_emission_activity)

    emission_activity_name_col = '배출활동 코드명'
    allowed_emission_activity_name = ['고체연료연소', '기체연료연소', '액체연료연소', '연료전지']
    is_allowed_emission_activity_name = check_isin(
        series=data[emission_activity_name_col].str.strip(), values=allowed_emission_activity_name)

    is_selected = (
        is_right_year & is_energy_generation_workplace & 
        is_allowed_emission_activity & is_allowed_emission_activity_name)
    
    values = [
        '관리업체명', '사업장 일련번호', '사업장명', 
        '공정명', '생산품명', '배출활동분류', '배출활동 코드', '배출활동 코드명', 
        '활동자료 코드', '활동자료 코드명', '배출량(tCO2eq)', '에너지사용량(TJ)', '생산량', 
        '생산단위코드', '생산단위', '배출원단위(tCO2eq/생산단위)', '에너지원단위(TJ/생산단위)']

    main_process = data.loc[is_selected, values].copy()
    main_process.to_excel('사업장 주요공정.xlsx')
    save_pickle(main_process, 'energy_generation_workplace_process.pkl', Path(os.getcwd()))

    products = main_product['사업장 주요 생산제품 또는 처리물질'].str.split('|')

    return new_total

def arrange_energy_generation_emission_equipment_in_data_5(
    input_filename:str, output_filename:str)->None:
    ''' 에너지공급 시설에 해당하는 시설에 대해 배출활동자료 선별
    '''
    data = pd.read_csv(data_path/input_filename, encoding='cp949')

    is_allowed_biz = check_isin(series=data[BIZ_COL], values=ALLOWED_BIZ)

    emission_activity_col = '배출활동분류'
    allowed_emission_activity = ['고정연소', '기타']
    is_allowed_emission_activity = check_isin(
        series=data[emission_activity_col].str.strip(), values=allowed_emission_activity)

    emission_activity_name_col = '배출활동명'
    allowed_emission_activity_name = ['고체연료연소', '기체연료연소', '액체연료연소', '연료전지']
    is_allowed_emission_activity_name = check_isin(
        series=data[emission_activity_name_col].str.strip(), values=allowed_emission_activity_name)

    equipment_col = '배출시설 코드명'
    allowed_equipment = [
        '열병합 발전시설', '화력 발전시설', '연료전지', '소각보일러', '발전용 내연기관']
    is_allowed_equipment = check_isin(series=data[equipment_col].str.strip(), values=allowed_equipment)

    is_etc_fuelcell = check_isin(series=data[equipment_col].str.strip(), values=['개질공정'])
    is_etc_fuelcell = is_etc_fuelcell & check_any_and_must_not_have_keywords(data['자체 시설명'], ['연료전지'], None)

    # 열공급 관련 업종의 보일러 설비 반영
    is_peak_load_boiler = check_isin(series=data[equipment_col].str.strip(), values=['일반 보일러시설'])
    is_peak_load_boiler = is_peak_load_boiler & check_isin(
        series=data[BIZ_COL].str.strip(), values=['증기, 냉ㆍ온수 및 공기 조절 공급업'])

    # 기타 SRF 보일러 추가
    is_etc_boiler = check_isin(series=data[equipment_col].str.strip(), values=['기타'])
    is_etc_boiler = is_etc_boiler & check_isin(series=data[BIZ_COL].str.strip(), values=['증기, 냉ㆍ온수 및 공기 조절 공급업'])
    is_etc_boiler = is_etc_boiler & check_any_and_must_not_have_keywords(data['자체 시설명'], ['보일러'], None)
   
    is_data_selected = (
        is_allowed_biz & 
        is_allowed_emission_activity & is_allowed_emission_activity_name & 
        is_allowed_equipment | is_etc_fuelcell | is_etc_boiler | is_peak_load_boiler)

    usecols = [
        '대상연도', '관장기관', '관리업체명', '지정업종', '사업장 일련번호', '사업장명', '사업장 대표업종 코드명', 
        '사업장 소재지', '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '시설규모', '배출활동분류', 
        '배출활동명', '방법론 최소 Tier', '방법론 적용 Tier', '활동자료 코드', '활동자료 코드명', '활동자료 대분류명', 
        '활동자료 중분류명', '활동자료명', '활동자료 단위코드', '활동자료 단위코드명', '활동자료 사용량', '매개변수 종류', 
        '매개변수 정형화명칭', '매개변수명(비정형)', '파라미터명', 
        '정렬순서', '매개변수 단위', '매개변수값', '온실가스 배출량(tCO2eq)']

    # debug 용도
    # data.loc[is_data_selected, usecols].to_excel(output_filename)

    activity_amount_col = '활동자료 사용량'
    parameter_value_col = '매개변수값'

    is_activity_parameter = data[activity_amount_col] == data[parameter_value_col]
    is_data_selected = is_data_selected & is_activity_parameter
    activities = data.loc[is_data_selected, usecols].copy()

    # 에너지원별 정보인 '활동자료 코드명'은 제외
    index = [
        '관리업체명', '사업장 일련번호', '사업장명', '배출시설 일련번호', '배출시설 코드명', '자체 시설명']
    values = ['온실가스 배출량(tCO2eq)']

    activities_grouped = activities.groupby(index)[values].sum()
    activities_grouped.to_excel(output_filename)

    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(activities_grouped, output_filename_pkl , Path(os.getcwd()))


def select_energy_generation_emission_equipment_in_data_5(
    input_filename:str, output_filename:str, sectors = None)->None:
    ''' 에너지공급 시설에 해당하는 시설에 대해 배출활동자료 선별
    '''
    data = pd.read_csv(data_path/input_filename, encoding='cp949')

    # sector 조건이 입력되지 않으면 전체 업종 허용
    if sectors is None:
        sectors = data[BIZ_COL].unique().tolist()

    is_right_sectors = check_isin(series=data[BIZ_COL], values=sectors)

    emission_activity_col = '배출활동분류'
    allowed_emission_activity = ['고정연소', '기타']
    is_allowed_emission_activity = check_isin(
        series=data[emission_activity_col].str.strip(), values=allowed_emission_activity)

    emission_activity_name_col = '배출활동명'
    allowed_emission_activity_name = ['고체연료연소', '기체연료연소', '액체연료연소', '연료전지']
    is_allowed_emission_activity_name = check_isin(
        series=data[emission_activity_name_col].str.strip(), values=allowed_emission_activity_name)

    equipment_col = '배출시설 코드명'
    allowed_equipment = [
        '열병합 발전시설', '화력 발전시설', '연료전지', '소각보일러', '발전용 내연기관']
    is_allowed_equipment = check_isin(series=data[equipment_col].str.strip(), values=allowed_equipment)

    is_etc_fuelcell = check_isin(series=data[equipment_col].str.strip(), values=['개질공정'])
    is_etc_fuelcell = is_etc_fuelcell & check_any_and_must_not_have_keywords(data['자체 시설명'], ['연료전지'], None)

    # 열공급 관련 업종의 보일러 설비 반영
    is_peak_load_boiler = check_isin(series=data[equipment_col].str.strip(), values=['일반 보일러시설'])
    is_peak_load_boiler = is_peak_load_boiler & check_isin(
        series=data[BIZ_COL].str.strip(), values=['증기, 냉ㆍ온수 및 공기 조절 공급업'])

    # 기타 SRF 보일러 추가
    is_etc_boiler = check_isin(series=data[equipment_col].str.strip(), values=['기타'])
    is_etc_boiler = is_etc_boiler & check_isin(series=data[BIZ_COL].str.strip(), values=['증기, 냉ㆍ온수 및 공기 조절 공급업'])
    is_etc_boiler = is_etc_boiler & check_any_and_must_not_have_keywords(data['자체 시설명'], ['보일러'], None)
   
    is_data_selected = (
        is_right_sectors & 
        is_allowed_emission_activity & is_allowed_emission_activity_name & 
        is_allowed_equipment | is_etc_fuelcell | is_etc_boiler | is_peak_load_boiler)

    usecols = [
        '대상연도', '관장기관', '관리업체명', '지정업종', '사업장 일련번호', '사업장명', '사업장 대표업종 코드명', 
        '사업장 소재지', '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '시설규모', '배출활동분류', 
        '배출활동명', '방법론 최소 Tier', '방법론 적용 Tier', '활동자료 코드', '활동자료 코드명', '활동자료 대분류명', 
        '활동자료 중분류명', '활동자료명', '활동자료 단위코드', '활동자료 단위코드명', '활동자료 사용량', '매개변수 종류', 
        '매개변수 정형화명칭', '매개변수명(비정형)', '파라미터명', 
        '정렬순서', '매개변수 단위', '매개변수값', '온실가스 배출량(tCO2eq)']

    # debug 용도
    # data.loc[is_data_selected, usecols].to_excel(output_filename)

    activity_amount_col = '활동자료 사용량'
    parameter_value_col = '매개변수값'

    is_activity_parameter = data[activity_amount_col] == data[parameter_value_col]
    is_data_selected = is_data_selected & is_activity_parameter
    activities = data.loc[is_data_selected, usecols].copy()

    activities.to_excel(output_filename)
    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(activities, output_filename_pkl , Path(os.getcwd()))    

    workplace_no_col = '사업장 일련번호'
    activities = activities.reset_index()
    workplace_list = activities[workplace_no_col].unique().tolist()
    output_filename_workplace_pkl = output_filename[:output_filename.rfind('.')]+'_wp.pkl'
    save_pickle(workplace_list, output_filename_workplace_pkl, Path(os.getcwd()))


def sum_emission_by_activity_in_data_5(
    input_filename:str, output_filename:str)->None:
    ''' 에너지공급 시설의 배출활동 배출량 합산
    '''
    data = pickle_first(input_filename, Path(os.getcwd()))
    # 에너지원별 정보인 '활동자료 코드명'은 제외
    index = [
        '관리업체명', '사업장 일련번호', '사업장명', '배출시설 일련번호', '배출시설 코드명', '자체 시설명']
    values = ['온실가스 배출량(tCO2eq)']

    data_grouped = data.groupby(index)[values].sum()
    data_grouped.to_excel(output_filename)

    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(data_grouped, output_filename_pkl , Path(os.getcwd()))


def select_energy_generation_equipment_data_6(
    input_filename:str, output_filename:str, sectors = None)->None:
    ''' 에너지공급 시설에 해당하는 시설에 대해 배출량 자료 선별
    '''
    
    data = pd.read_csv(data_path/input_filename, encoding='cp949')

    year_col = '대상연도'
    target_year = 2020
    is_right_year = data[year_col] == target_year

    if sectors is None:
        sectors = data[BIZ_COL].unique().tolist()

    is_right_sectors = check_isin(series=data[BIZ_COL], values=sectors)

    emission_activity_col = '배출활동분류'
    allowed_emission_activity = ['고정연소', '기타']
    is_allowed_emission_activity = check_isin(
        series=data[emission_activity_col].str.strip(), values=allowed_emission_activity)

    emission_activity_name_col = '배출활동 코드명'
    allowed_emission_activity_name = ['연료전지', '기체연료연소', '액체연료연소', '고체연료연소', '폐기물 소각']
    
    is_allowed_emission_activity_name = check_isin(
        series=data[emission_activity_name_col].str.strip(), values=allowed_emission_activity_name)

    is_data_selected = (
        is_right_year & is_right_sectors & is_allowed_emission_activity & is_allowed_emission_activity_name)

    target_df = data.loc[is_data_selected].copy()
    target_df.to_excel(output_filename)
    save_pickle(target_df, output_filename[:output_filename.rfind('.')]+'.pkl', Path(os.getcwd()))

def process_production_amount_data_6(input_filename:str, output_filename:str)->None:
    ''' 공정 파일(6번)에서 생산량 수치가 중복되지 않도록 정리.
    생산량을 공유하는 자료는 생산량, 배출원단위, 에너지원단위가 동일하다는 특징을 이용
    '''
    
    data = pickle_first(input_filename, Path(os.getcwd()))

    # groupby
    index = ['관리업체명', '사업장 일련번호',
             '사업장명', '공정명', '생산품명', '배출활동분류', '생산단위']
    values = ['생산량', '배출량(tCO2eq)', '에너지사용량(TJ)',
              '배출원단위(tCO2eq/생산단위)', '에너지원단위(TJ/생산단위)']

    # debug 용도
    # data = data.loc[data['사업장명'] == '씨엔씨티에너지 주식회사'].copy()

    result = []

    # 배출원별 생산량을 공유하는 경우를 처리
    for name, frame in data.groupby(index)[values]:

        emission = 0
        energy_consumption = 0
        production = 0

        index_dict = {key: value for key, value in zip(index, name)}
        nunique_production_value = pd.Series.nunique(frame['생산량'])

        # 생산량(전력, 열) 계산
        for i in range(len(frame)):

            emission += frame['배출량(tCO2eq)'].iloc[i]
            energy_consumption += frame['에너지사용량(TJ)'].iloc[i]

            emission_intensity_in_data = frame['배출원단위(tCO2eq/생산단위)'].iloc[i]
            emission_intensity_calculated = frame['배출량(tCO2eq)'].iloc[i] / \
                frame['생산량'].iloc[i]

            energy_intensity_in_data = frame['에너지원단위(TJ/생산단위)'].iloc[i]
            energy_intensity_calculated = frame['에너지사용량(TJ)'].iloc[i] / \
                frame['생산량'].iloc[i]

            # 초기값 저장
            if i == 0:
                production += frame['생산량'].iloc[i]
            else:
                # 생산량 수치를 공유하고 있는 경우에는 생산량을 더하지 않음
                # 생산량 수치를 공유하고 있는 경우 판단 기준
                # 1)이전값과 현재값의 생산량, 배출원단위, 에너지원단위가 동일
                # 2)자료상의 수치(에너지사용량, 배출량, 생산량)를 통해 계산한 원단위(전력, 배출)가 자료 상의 원단위와 1% 이상 차이
                # 1%는 임의로 정한 값으로 아주 작은 차이를 의미
                # 활동자료별 계산 원단위와 자료 상의 원단위가 많이 차이난다는 것은 원단위를 제품별로 계산했다는 의미임

                # is_emission_different = frame['배출량(tCO2eq)'].iloc[i] != frame['배출량(tCO2eq)'].iloc[i-1]
                # is_energy_consumption_different = frame['에너지사용량(TJ)'].iloc[i] != frame['에너지사용량(TJ)'].iloc[i-1]
                is_production_different = frame['생산량'].iloc[i] != frame['생산량'].iloc[i-1]

                try:
                    is_energy_intensity_same = \
                        (energy_intensity_calculated - energy_intensity_in_data) / \
                        energy_intensity_in_data < 0.01

                    is_emission_intensity_same = \
                        (emission_intensity_calculated - emission_intensity_in_data) / \
                        emission_intensity_in_data < 0.01

                # divide by zero 등 오류 처리
                except:
                    is_energy_intensity_same = is_emission_intensity_same = None

                if is_production_different and is_energy_intensity_same and is_emission_intensity_same:
                    production += frame['생산량'].iloc[i]

        # 생산량 고유값이 유일하면
        if nunique_production_value == 1:
            emission_intensity = emission_intensity_in_data
            energy_intensity = energy_intensity_in_data
        else:
            emission_intensity = emission / production
            energy_intensity = energy_consumption / production

        data = {
            '생산량': production,
            '배출량(tCO2eq)': emission,
            '에너지사용량(TJ)': energy_consumption,
            '개수': len(frame),
            '생산량고유개수': nunique_production_value,
            '배출원단위(tCO2eq/생산단위)': emission_intensity,
            '에너지원단위(TJ/생산단위)': energy_intensity}
        index_dict.update(data)

        result.append(pd.DataFrame(data=index_dict, index=[0]))

    production_amount_arrangement_result1 = pd.concat(result).set_index(index)
    production_amount_arrangement_result1.to_excel(output_filename)
    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(production_amount_arrangement_result1, output_filename_pkl , Path(os.getcwd()))


def eliminate_duplicated_production_amount_data_6(input_filename:str, output_filename:str)->None:
    ''' 중복으로 정리된 생산량 수치 삭제
    1차 정리 결과는 공정과 생산품명, 배출활동분류가 동일한 경우의 생산량을 정리한 것임
    연료전지와 다른 발전시설이 열, 전기 생산량을 공유하는 경우와 같이
    공정과 생산품명이 동일하나 배출활동분류가 다른 경우에는 생산량이 중복 기재되고 있으므로 이를 제거
    (배출활동분류 : 발전시설은 '고정연소' 등, 연료전지는 '기타')
    '''

    data = pickle_first(input_filename, Path(os.getcwd()))
    # stage1.to_excel('test_gp1.xlsx')

    # 2차 정리
    # 

    data = data.reset_index()
    index = ['관리업체명', '사업장 일련번호', '사업장명', '공정명', '생산품명']
    values = ['생산량', '배출량(tCO2eq)', '에너지사용량(TJ)',
              '배출원단위(tCO2eq/생산단위)', '에너지원단위(TJ/생산단위)']

    result = []

    # 1)배출원단위, 에너지원단위가 동일한 경우에는 중복되는 생산량을 제거
    for name, frame in data.groupby(index)[values]:

        # 생산량(전력, 열) 계산
        for i in range(len(frame)):

            # 첫 행은 아무 처리도 하지 않음
            if i == 0:
                pass
            else:

                is_emission_intensity_same = frame['배출원단위(tCO2eq/생산단위)'].iloc[i] == frame['배출원단위(tCO2eq/생산단위)'].iloc[i-1]
                is_energy_intensity_same = frame['에너지원단위(TJ/생산단위)'].iloc[i] == frame['에너지원단위(TJ/생산단위)'].iloc[i-1]

                if is_emission_intensity_same and is_energy_intensity_same:
                    frame['생산량'].iloc[i] = np.nan
                    frame['비고'] = '중복되는 생산량 수치는 삭제처리됨'

        result.append(frame)

    data = pd.concat(result).set_index(index)
    
    data.to_excel(output_filename)
    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(data, output_filename_pkl, Path(os.getcwd()))

def convert_production_unit_data_6(input_filename:str, output_filename:str)->None:
    ''' 공정 파일 정리 결과의 단위를 MWh로 환산
    '''
    data = pickle_first(input_filename, Path(os.getcwd()))
    # stage1.to_excel('test_gp1.xlsx')
   
    data = data.apply(lambda row: convert_production_units_including_ton(row), axis=1)
    data.to_excel(output_filename)
    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(data, output_filename_pkl, Path(os.getcwd()))

def seperate_fuelcell_in_process_info_in_data_6():
    ''' 6번 자료 가공결과에서 연료전지 분리 '''

    filename = 'process_result2_in_data6.pkl'
    data = pickle_first(filename, Path(os.getcwd()))

    values_rename = {
        '전력':'전기', '전기, 열':'전기, 열', '열':'열', '열(온수)':'열', '열, 전기':'전기, 열', 
        '전기':'전기', '증기':'증기', '스팀':'증기', '열및전기':'전기, 열', '열생산':'열', '전기생산':'전기', 
        '열 및 전기':'전기, 열', '열(스팀)':'증기', '중온수':'열', '지역난방(온수)':'열', '열(중온수)':'열', 
        '지역난방순환수(중온수)':'열', '전기 및 열':'전기, 열', '전기, 증기 생산':'전기, 열', '전기, 증기 판매':'전기, 열',
         '온수':'열', '전기,열':'전기, 열', '전기(송전량)':'전기', '송전량':'전기', 'CHP(열)':'열', 'CHP(전기)':'전기', 
         '#1 PLBwg(열, 영통)':'열', '#1 PLBwg(열, 장안)':'열', '#2 PLBwg(열, 장안)':'열', '#2 PLBwg(열, 영통)':'열', 
         '#1 PLBs':'열', '#1 PLBw':'열', '#2 PLBs':'열', '#2 PLBw':'열', '우드칩(열)':'열', '우드칩(전기)':'전기', 
         '열병합발전시설/온수':'열', '열병합발전시설/전기':'전기', '일반보일러(#1 PLBs)/온수':'열', '일반보일러(#2 PLBs)/온수':'열', 
         ' 전기':'전기', 'Superheated Steam(124KG)':'증기', 'High pressure Steam(43KG)':'증기'
    }

    data = data.reset_index()
    data['생산품명_변경'] = data['생산품명'].apply(lambda x:values_rename.get(x, x))

    def change_unit_amount(row):
        ''' 6번 공정데이터의 가공결과의 생산량 단위, 수량 수정 ''' 

        if row['생산단위'] == 'TJ':
            row['생산단위_변경'] = 'MWh'
            row['생산량(MWh)'] = row['생산량'] * convert_units('TJ', 'MWh')
        
        elif row['생산단위'] == 'GJ':
            row['생산단위_변경'] = 'MWh'
            row['생산량(MWh)'] = row['생산량'] * convert_units('GJ', 'MWh')

        # ton은 열량으로 환산하지 않고 그대로 유지
        elif row['생산단위'] == 'ton':
            row['생산단위_변경'] = 'ton'
            row['생산량(MWh)'] = row['생산량']

        elif row['생산단위'] == 'kWh':
            row['생산단위_변경'] = 'MWh'
            row['생산량(MWh)'] = row['생산량'] * convert_units('kWh', 'MWh')

        elif row['생산단위'] == 'MJ':
            row['생산단위_변경'] = 'MWh'
            row['생산량(MWh)'] = row['생산량'] * convert_units('MJ', 'MWh')


        elif row['생산단위'] == 'GKCAL':
            row['생산단위_변경'] = 'MWh'
            row['생산량(MWh)'] = row['생산량'] * convert_units('GKCAL', 'MWh')

        elif row['생산단위'] == 'MWh':
            row['생산단위_변경'] = 'MWh'
            row['생산량(MWh)'] = row['생산량']

        return row

    data = data.apply(lambda row:change_unit_amount(row), axis=1)
    data.to_excel('process_result2_생산량변경_in_data6.xlsx')
    
    # 단위가 MWh인 데이터만 정리
    is_MWh = data['생산단위_변경'] == 'MWh'
    is_ton = data['생산단위_변경'] == 'ton'

    # data_pivot_MWh = pd.pivot_table(
    #     data.loc[is_MWh], index=['사업장 일련번호', '사업장명', '배출활동분류'], values=['생산량(MWh)', '배출량(tCO2eq)'],
    #     columns = ['생산품명_변경'], aggfunc='sum')

    data_pivot_MWh = pd.pivot_table(
        data.loc[is_MWh], index=['사업장 일련번호', '사업장명', '배출활동분류'], values=['생산량(MWh)', '배출량(tCO2eq)'],
         aggfunc='sum')


    data_pivot_MWh.to_excel('생산량_배출량_pivot_MWh_data6.xlsx')

    data_pivot_ton = pd.pivot_table(
        data.loc[is_ton].rename(columns={'생산량(MWh)':'생산량(ton)'}), 
        index=['사업장 일련번호', '사업장명', '배출활동분류'], values=['생산량(ton)', '배출량(tCO2eq)'],
        aggfunc='sum')

    data_pivot_ton.to_excel('생산량_배출량_pivot_ton_data6.xlsx')

    # 연료전지 사업장 분리
    data_pivot_MWh_uns = data_pivot_MWh.unstack('배출활동분류')
    data_pivot_MWh_uns = data_pivot_MWh_uns.swaplevel(1, 0, axis=1)
    data_pivot_MWh_uns = data_pivot_MWh_uns.sort_index(axis=1)
    data_pivot_MWh_uns.to_excel('생산량_배출량_pivot_MWh_연료전지분리_data6.xlsx')
    print(1)

def compare_emission_activity_process_data_56(
    input_filenames:List[str], output_filename:str)->None:
    ''' 배출활동과 공정 파일의 정리 결과 비교
    '''

    frames = [pickle_first(input_filename, Path(os.getcwd())) for input_filename in input_filenames] 

    # 배출활동 확인
    if '배출시설 일련번호' in frames[0].columns:
        process, activities  = frames[0], frames[1]
    else:
        process, activities = frames[1], frames[0]

    # 배출량 합계 확인
    index = ['관리업체명', '사업장 일련번호', '사업장명']
    activities_grouped = activities.groupby(index)[['온실가스 배출량(tCO2eq)']].sum()
    process_grouped = process.groupby(index)[['배출량(tCO2eq)']].sum()

    comparison = pd.concat(
        [activities_grouped.rename(columns={'온실가스 배출량(tCO2eq)':'배출활동 배출량(tCO2eq)'}), 
        process_grouped.rename(columns={'배출량(tCO2eq)':'공정 배출량(tCO2eq)'})],
        axis=1)
    comparison.to_excel('comparison.xlsx')

    process = process.reset_index()
    activities = activities.reset_index()
    comparison = comparison.reset_index()

    result = []
    workplace_number_col = '사업장 일련번호'
    usecols_activities = [
        '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '온실가스 배출량(tCO2eq)']
    usecols_process = [
        '공정명', '생산품명', '배출활동분류', '생산단위', '생산량', '배출량(tCO2eq)', '에너지사용량(TJ)', '생산량(MWh)']

    for workplace in comparison[workplace_number_col]:
        is_right_workplace_process = process[workplace_number_col] == workplace
        is_right_workplace_activities = activities[workplace_number_col] == workplace
        is_right_workplace_comparison = comparison[workplace_number_col] == workplace

        merged = pd.concat(
            [comparison.loc[is_right_workplace_comparison].reset_index(drop=True).assign(blank = np.nan),
            activities.loc[is_right_workplace_activities].reset_index(drop=True)[usecols_activities].assign(blank = np.nan),
            process.loc[is_right_workplace_process].reset_index(drop=True)[usecols_process]], axis=1)
    
        result.append(merged)

    data = pd.concat(result).reset_index(drop=True)
    data.to_excel(output_filename)
    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(data, output_filename_pkl, Path(os.getcwd()))


def merge_activity_process_data_56(
    input_filenames:List[str], output_filename:str)->None:
    ''' 배출활동과 공정 파일의 정리 결과 비교
    '''

    frames = [pickle_first(input_filename, Path(os.getcwd())) for input_filename in input_filenames] 

    # 배출활동 확인
    if '배출시설 일련번호' in frames[0].columns:
        process, activities  = frames[0], frames[1]
    else:
        process, activities = frames[1], frames[0]

    # 배출량 합계 확인
    index = ['관리업체명', '사업장 일련번호', '사업장명']
    activities_grouped = activities.groupby(index)[[('온실가스 배출량(tCO2eq)', '')]].sum()
    process_grouped = process.groupby(index)[['배출량(tCO2eq)']].sum()

    comparison = pd.concat(
        [activities_grouped.rename(columns={'온실가스 배출량(tCO2eq)':'배출활동 배출량(tCO2eq)'}), 
        process_grouped.rename(columns={'배출량(tCO2eq)':'공정 배출량(tCO2eq)'})],
        axis=1)
    comparison.to_excel('comparison.xlsx')

    process = process.reset_index()
    activities = activities.reset_index()
    comparison = comparison.reset_index()

    result = []
    workplace_number_col = '사업장 일련번호'
    usecols_activities = [
        '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '온실가스 배출량(tCO2eq)', '연료열량_총발열량(TJ)', 
        '연료열량_순발열량(TJ)']
    usecols_process = [
        '공정명', '생산품명', '배출활동분류', '생산단위', '생산량', '배출량(tCO2eq)', '에너지사용량(TJ)', '생산량(MWh)']

    for workplace in comparison[workplace_number_col]:
        is_right_workplace_process = process[workplace_number_col] == workplace
        is_right_workplace_activities = activities.loc[:, workplace_number_col] == workplace
        is_right_workplace_comparison = comparison.loc[:, workplace_number_col] == workplace

        activities_sel = activities.loc[is_right_workplace_activities][usecols_activities].copy()
        
        # MultiIndex 제거
        activities_sel.columns = activities_sel.columns.get_level_values(0)
        activities_sel = activities_sel.reset_index(drop=True)

        merged = pd.concat(
            [comparison.loc[is_right_workplace_comparison].reset_index(drop=True).assign(blank = np.nan),
            activities_sel.assign(blank = np.nan),
            process.loc[is_right_workplace_process].reset_index(drop=True)[usecols_process]], axis=1)
    
        result.append(merged)

    data = pd.concat(result).reset_index(drop=True)
    data.to_excel(output_filename)
    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(data, output_filename_pkl, Path(os.getcwd()))

def delete_same_equipment_data_56(input_filenames:List[str], output_filename:str)->pd.DataFrame:
    ''' 2개의 파일에 존재하는 공통 데이터를 삭제 '''

    equipment_no_col = '배출시설 일련번호'
    frames = [pickle_first(input_filename, Path(os.getcwd())) for input_filename in input_filenames] 
    equipments = [frame[equipment_no_col].unique().tolist() for frame in frames]
    lengths = [len(equipment) for equipment in equipments]
    
    max_length_frames = frames[lengths.index(max(lengths))]
    min_length_equipment_list = equipments[lengths.index(min(lengths))]

    # 중복 제거
    min_length_equipment_list = list(set(min_length_equipment_list)) 
    is_duplicated = max_length_frames[equipment_no_col].isin(min_length_equipment_list)
    data = max_length_frames.drop(max_length_frames.loc[is_duplicated].index)
    data.to_excel(output_filename)
    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(data, output_filename_pkl, Path(os.getcwd()))




check_conversion_unit = {}


def compare_emission_activity_process_data_56(
    input_filenames:List[str], output_filename:str)->None:
    ''' 배출활동과 공정 파일의 정리 결과 비교
    '''



def unstack_parameter_variable_names_in_data_5(
    frame:pd.DataFrame, common_value_cols:List[str], index_cols:List[str], 
    parameter_name_col:str)->pd.DataFrame:
    
    '''
    5번 데이터의 배출활동에 대해 '매개변수명(비정형)' 컬럼을 unstack
    입력 frame은 index가 설정되지 않은 상태로 입력되는 것을 가정
    
    '''
    # unstack 전에 값이 공통되는 컬럼은 index로 지정
    # parameter_name_col은 unstack 대상이므로 index에 포함
    frame = frame.set_index(common_value_cols+[parameter_name_col])
    frame = frame.sort_index()
    try:
        frame_uns = frame.unstack(parameter_name_col)
        frame_uns_swap = frame_uns.swaplevel(0, 1, axis=1)
        frame_uns_swap = frame_uns_swap.sort_index(axis=1, level=0)
        frame_uns_swap = frame_uns_swap.reset_index()
        frame_uns_swap = frame_uns_swap.set_index(index_cols)

        return frame_uns_swap

    except ValueError:
        raise ValueError


def arrange_activities_in_data_5(input_filename:str, output_filename:str)->None:
    ''' 조건에 해당하는 활동 자료를 정리 (배출량, 배출계수 등)
    '''
    data = pickle_first(input_filename, data_path)

    conditions = DataConditions(
        year=2020, 
        equipment_divs = [
        '개질공정', '발전용 내연기관', '연료전지', '열병합 발전시설', '일반 보일러시설', '화력 발전시설', '기타'],
        activity_divs = ['고정연소'],
        activity_names = ['연료전지'])

    data = data.loc[is_selected(data, conditions)].copy()

    result = []

    # 공백제거
    data['매개변수명(비정형)'] = data['매개변수명(비정형)'].str.replace(' ', '')

    # 용어 변경
    data.loc[data['매개변수명(비정형)'] == 'N2O',
                  '매개변수명(비정형)'] = '온실가스배출계수(N2O)'
    data.loc[data['매개변수명(비정형)'] == 'CO2',
                  '매개변수명(비정형)'] = '온실가스배출계수(CO2)'
    data.loc[data['매개변수명(비정형)'] == 'CH4',
                  '매개변수명(비정형)'] = '온실가스배출계수(CH4)'

    common_value_cols = [
        '관리업체명', '사업장 일련번호', '사업장명',
        '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '활동자료 코드명', 
        '활동자료명', '배출활동명', '활동자료 사용량', '온실가스 배출량(tCO2eq)']
    
    index_cols = [col for col in common_value_cols
        if col not in ['활동자료 사용량', '온실가스 배출량(tCO2eq)', '매개변수명(비정형)']]

    parameter_name_col = '매개변수명(비정형)'
    values = [
        '매개변수명(비정형)', '매개변수 단위', '매개변수값', '매개변수적용 Tier', '정렬순서']

    # 세로형태로 저장된 데이터를 가로형태로 변환
    for name, frame in tqdm(data[common_value_cols+values].groupby(common_value_cols)):

        serial = frame['배출시설 일련번호'].iloc[0]
        
        # unstack & swap
        try:
            frame_uns_swap = unstack_parameter_variable_names_in_data_5(
                frame, common_value_cols, index_cols, parameter_name_col)
            result.append(frame_uns_swap)

        # unstack 실패(중복 등)
        except ValueError:

            try:

                # '정렬순서' 정보를 이용하여 데이터를 분리
                # 동일한 내용으로 구성되었는지 확인
                items_per_block = frame['정렬순서'].max()
                number_of_blocks = len(frame)/items_per_block

                # 동일한 개수의 블록으로 구성된 경우
                if (number_of_blocks >= 2) and number_of_blocks.is_integer():
                    print(f'serial number = {serial}, 동일 개수 블록 처리')

                    start_indexs = [
                        index*items_per_block for index in range(int(number_of_blocks))]

                    for start_index in start_indexs:
                        sub_frame = frame.iloc[start_index:start_index +
                                               items_per_block]
                        frame_uns_swap = unstack_parameter_variable_names_in_data_5(
                            sub_frame, common_value_cols, index_cols, parameter_name_col)
                        
                        result.append(frame_uns_swap)

                    continue

                # frame = frame.reset_index()
                # 동일한 개수의 블록으로 구성되지 않은 경우
                # 중복컬럼 확인
                only_fuel_col_duplicated = pd.Series.unique(
                    frame.loc[frame['매개변수명(비정형)'].duplicated(), '매개변수명(비정형)'])[0] == '연료사용량'

                if only_fuel_col_duplicated:
                    # 활동자료 사용량과 수치가 다른 연료사용량 삭제
                    # print(f'자료 확인 시설일련번호 : {}, 중복회피를 위해 연료사용량 행 중 일부 삭제}')
                    print(f'serial number = {serial}, 연료사용량 중복 기재 처리')
                    is_value_different = frame['활동자료 사용량'] != frame['매개변수값']
                    is_fuel_duplicated = frame['매개변수명(비정형)'].duplicated()
                    frame.drop(
                        frame.loc[is_value_different & is_fuel_duplicated].index, axis=0, inplace=True)
                    frame_uns_swap = unstack_parameter_variable_names_in_data_5(
                        frame, common_value_cols, index_cols, parameter_name_col)
                    result.append(frame_uns_swap)

                    continue

                only_indirect_coeffi_duplicated = pd.Series.unique(
                    frame.loc[frame['매개변수명(비정형)'].duplicated(), '매개변수명(비정형)'])[0] == '간접배출계수'

                # 간접배출계수 중복 처리
                if only_indirect_coeffi_duplicated:
                    print(f'serial number = {serial}, 간접배출계수 중복 처리')
                    for i in range(len(frame)):
                        # i=0
                        if 'N2O' in frame['매개변수 단위'].iloc[i]:
                            frame['매개변수명(비정형)'].iloc[i] = '온실가스배출계수(N2O)'

                        elif 'CH4' in frame['매개변수 단위'].iloc[i]:
                            frame['매개변수명(비정형)'].iloc[i] = '온실가스배출계수(CH4)'

                        elif 'CO2' in frame['매개변수 단위'].iloc[i]:
                            frame['매개변수명(비정형)'].iloc[i] = '온실가스배출계수(CO2)'

                    frame_uns_swap = unstack_parameter_variable_names_in_data_5(
                        frame, common_value_cols, index_cols, parameter_name_col)
                    result.append(frame_uns_swap)

                    # # debug 용도
                    # break
                    continue

                # 예외 처리에 실패한 경우 매개변수 '매개변수 단위' 컬럼을 이용하여 고유하게 변경
                frame = frame.reset_index()
                frame['매개변수명(비정형)'] = frame['매개변수명(비정형)'] + \
                    '_' + frame['매개변수 단위'].fillna('')

                frame_uns_swap = unstack_parameter_variable_names_in_data_5(
                        frame, common_value_cols, index_cols, parameter_name_col)
                result.append(frame_uns_swap)

            # 중복값이 발생하면 '정렬순서'를 이용하여 index를 고유하게 수정
            except ValueError:
                frame = frame.reset_index()
                frame['매개변수명(비정형)'] = frame['매개변수명(비정형)'] + \
                    '_' + frame['정렬순서'].astype(str)
                frame_uns_swap = unstack_parameter_variable_names_in_data_5(
                        frame, common_value_cols, index_cols, parameter_name_col)
                result.append(frame_uns_swap)


    # 정리결과 저장
    data = pd.concat(result)
    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(data, output_filename_pkl, Path(os.getcwd()))

def arrange_parameter_col_in_data5(data:pd.DataFrame)->pd.DataFrame:
    '''
    매개변수명(비정형) 컬럼에 대한 전처리
    공백을 정리하거나, 이름을 가공하기 용이하도록 변경
    '''
    # 공백제거
    data['매개변수명(비정형)'] = data['매개변수명(비정형)'].str.replace(' ', '')

    # 용어 변경
    data.loc[data['매개변수명(비정형)'] == 'N2O',
                  '매개변수명(비정형)'] = '온실가스배출계수(N2O)'
    data.loc[data['매개변수명(비정형)'] == 'CO2',
                  '매개변수명(비정형)'] = '온실가스배출계수(CO2)'
    data.loc[data['매개변수명(비정형)'] == 'CH4',
                  '매개변수명(비정형)'] = '온실가스배출계수(CH4)'

    return data


def unstack_cols_in_data5(
    data:pd.DataFrame, unstack_info:UnstackInfo, activity_name_keyword:str, progress:bool=False)->pd.DataFrame:
    ''' frame에 대해서 unstack_info를 이용하여 데이터 정리 후 반환 '''

    result = []
    
    # 세로형태로 저장된 데이터를 가로형태로 변환
    # 각 행에 공통적으로 존재하는 데이터 common_value_cols를 index로 해서 groupby 적용
    if progress:
        iterables = tqdm(
            data[unstack_info.common_value_cols+unstack_info.different_value_cols].groupby(unstack_info.common_value_cols))
    else:
        iterables = \
            data[unstack_info.common_value_cols+unstack_info.different_value_cols].groupby(unstack_info.common_value_cols)

    for name, frame in iterables:

        # 각 frame에서 주요 정보 확인
        serial = frame[unstack_info.equipment_no_col].iloc[0]
        activity_emission = frame[unstack_info.activity_emission_col].iloc[0]
        equipment_emission = frame[unstack_info.equipment_emission_col].iloc[0]

        is_duplicated = True if activity_emission == equipment_emission else False

        # unstack & swap
        try:
            # 각 프레임에 대해 공통컬럼, 결과컬럼, unstack 컬럼을 이용하여 unstack 실시
            # 각 unstacked 결과는 unstacked 값에 따라 컬럼의 이름이 달라질 수 있음
            frame_uns_swap = unstack_parameter_variable_names_in_data_5(
                frame, unstack_info.common_value_cols, unstack_info.result_index_cols, unstack_info.unstack_col)
            result.append(frame_uns_swap)

        # unstack 실패(중복 등)
        except ValueError:

            try:

                # '정렬순서' 정보를 이용하여 데이터를 분리
                # 동일한 내용으로 구성되었는지 확인
                items_per_block = frame[unstack_info.order_col].max()
                number_of_blocks = len(frame)/items_per_block

                # 동일한 개수의 블록으로 구성된 경우
                if (number_of_blocks >= 2) and number_of_blocks.is_integer():
                    print(f'serial number = {serial}, 동일 개수 블록 처리')

                    start_indexs = [
                        index*items_per_block for index in range(int(number_of_blocks))]

                    for i, start_index in enumerate(start_indexs):
                        sub_frame = frame.iloc[start_index:start_index +
                                               items_per_block]
                        frame_uns_swap = unstack_parameter_variable_names_in_data_5(
                            sub_frame, unstack_info.common_value_cols, unstack_info.result_index_cols, unstack_info.unstack_col)
                        
                        if is_duplicated:
                            if i == 0:
                                result.append(frame_uns_swap)
                        else:
                            result.append(frame_uns_swap)

                    continue

                # frame = frame.reset_index()
                # 동일한 개수의 블록으로 구성되지 않은 경우
                # 중복컬럼 확인
                only_fuel_col_duplicated = pd.Series.unique(
                    frame.loc[frame[unstack_info.unstack_col].duplicated(), unstack_info.unstack_col])[0] == '연료사용량'

                if only_fuel_col_duplicated:
                    # 활동자료 사용량과 수치가 다른 연료사용량 삭제
                    # print(f'자료 확인 시설일련번호 : {}, 중복회피를 위해 연료사용량 행 중 일부 삭제}')
                    print(f'serial number = {serial}, 연료사용량 중복 기재 처리')
                    is_value_different = frame['활동자료 사용량'] != frame['매개변수값']
                    is_fuel_duplicated = frame[unstack_info.unstack_col].duplicated()
                    frame.drop(
                        frame.loc[is_value_different & is_fuel_duplicated].index, axis=0, inplace=True)
                    frame_uns_swap = unstack_parameter_variable_names_in_data_5(
                        frame, unstack_info.common_value_cols, unstack_info.result_index_cols, unstack_info.unstack_col)
                    result.append(frame_uns_swap)

                    continue

                only_indirect_coeffi_duplicated = pd.Series.unique(
                    frame.loc[frame[unstack_info.unstack_col].duplicated(), unstack_info.unstack_col])[0] == '간접배출계수'

                # 간접배출계수 중복 처리
                if only_indirect_coeffi_duplicated:
                    print(f'serial number = {serial}, 간접배출계수 중복 처리')
                    for i in range(len(frame)):
                        # i=0
                        if 'N2O' in frame['매개변수 단위'].iloc[i]:
                            frame[unstack_info.unstack_col].iloc[i] = '온실가스배출계수(N2O)'

                        elif 'CH4' in frame['매개변수 단위'].iloc[i]:
                            frame[unstack_info.unstack_col].iloc[i] = '온실가스배출계수(CH4)'

                        elif 'CO2' in frame['매개변수 단위'].iloc[i]:
                            frame[unstack_info.unstack_col].iloc[i] = '온실가스배출계수(CO2)'

                    frame_uns_swap = unstack_parameter_variable_names_in_data_5(
                        frame, unstack_info.common_value_cols, unstack_info.result_index_cols, unstack_info.unstack_col)
                    result.append(frame_uns_swap)

                    # # debug 용도
                    # break
                    continue

                # 예외 처리에 실패한 경우 매개변수 '매개변수 단위' 컬럼을 이용하여 고유하게 변경
                print(f'serial number = {serial}, 매개변수명을 단위를 이용하여 고유하게 수정')
                frame = frame.reset_index()
                frame[unstack_info.unstack_col] = frame[unstack_info.unstack_col] + \
                    '_' + frame['매개변수 단위'].fillna('')

                frame_uns_swap = unstack_parameter_variable_names_in_data_5(
                        frame, unstack_info.common_value_cols, unstack_info.result_index_cols, unstack_info.unstack_col)
                result.append(frame_uns_swap)

            # 중복값이 발생하면 '정렬순서'를 이용하여 index를 고유하게 수정
            except ValueError:
                frame = frame.reset_index()
                print(f'serial number = {serial}, 매개변수명을 정렬순서를 이용하여 고유하게 수정')
                frame[unstack_info.unstack_col] = frame[unstack_info.unstack_col] + \
                    '_' + frame[unstack_info.order_col].astype(str)
                frame_uns_swap = unstack_parameter_variable_names_in_data_5(
                        frame, unstack_info.common_value_cols, unstack_info.result_index_cols, unstack_info.unstack_col)
                result.append(frame_uns_swap)

    # 정리결과 저장. 처리한 데이터의 유형에 따라서 columns가 생성됨
    data = pd.concat(result)

    # 생성된 여러 columns 중에서 관심 대상 columns로 data frame을 구성
    columns = unstack_info.output_file_cols
    data_merge = pd.DataFrame(index=data.index, columns=columns)

    # data_index = [name for name in data.index.names]
    # data_index.remove('배출활동명')
    # data = data.reset_index().set_index(data_index)

    # activity_name_keyword에 따라 필요한 컬럼을 선택하고 이름을 변경
    if '연료연소' == activity_name_keyword:

        select_cols = unstack_info.unstacked_fuel_combustion_selected_cols
        data = data[select_cols].copy()
        rename_cols = unstack_info.unstacked_fuel_combustion_selected_rename_cols

    elif activity_name_keyword == '연료전지':

        select_cols = unstack_info.unstacked_fuelcell_selected_cols
        data = data[select_cols].copy()
        rename_cols = unstack_info.unstacked_fuelcell_selected_rename_cols
    else:
        select_cols = [('온실가스 배출량(tCO2eq)', '')]
        data = data[select_cols].copy()
        rename_cols = ['온실가스 배출량(tCO2eq)']

    data.columns = rename_cols
    try:
        data_merge.loc[:, rename_cols] = data.loc[:, rename_cols].copy()


    # multiIndex 가 고유하지 않아서 loc으로 값을 지정하지 못하는 경우
    except ValueError:

        for i in range(len(data_merge)):
            data_merge.iloc[i][rename_cols] = data.iloc[i][rename_cols].copy()

    finally:

        # 발열량 계산
        data_merge = data_merge.sort_values('온실가스 배출량(tCO2eq)', ascending=False)
        data_merge = data_merge.apply(lambda row:calc_heating_value(row), axis=1)
        
        # 컬럼위치 조정
        cols = list(data_merge.columns)
        reordered_cols = cols[-2:] + cols[:-2]
        data_merge = data_merge[reordered_cols]

        return data_merge

def arrange_main_activity_item_in_data_5(input_filename:str, output_filename:str)->None:
    ''' 조건에 해당하는 활동 자료에 대한 주요 사항(배출량, 배출계수 등) 정리
    '''
    data = pickle_first(input_filename, data_path)

    conditions = DataConditions(
        year=2020, 
        equipment_divs = [
        '개질공정', '발전용 내연기관', '연료전지', '열병합 발전시설', '일반 보일러시설', '화력 발전시설', '기타'],
        activity_divs = ['고정연소'],
        activity_names = ['연료전지'])

    data = data.loc[is_selected(data, conditions)].iloc[:1000].copy()

    # 공백 등 컬럼 전처리
    data = arrange_parameter_col_in_data5(data)

    unstack_info = UnstackInfo(
        unstack_col = '매개변수명(비정형)',
        common_value_cols = [
            '관리업체명', '사업장 일련번호', '사업장명',
            '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '활동자료 코드명', 
            '활동자료명', '배출활동명', '활동자료 사용량', '온실가스 배출량(tCO2eq)', '시설배출량'],
        different_value_cols = [
            '매개변수명(비정형)', '매개변수 단위', '매개변수값', '매개변수적용 Tier', '정렬순서'],
        result_value_cols = [
            '활동자료 사용량', '온실가스 배출량(tCO2eq)', '시설배출량', '매개변수명(비정형)'], 
        


        )

    data = unstack_cols_in_data5(data, unstack_info)
    data.to_excel(output_filename)
    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(data, output_filename_pkl, Path(os.getcwd()))


def calc_heating_value_emission_for_fuel(row:pd.Series)->pd.Series:
    ''' 연료투입량, 총발열량을 이용하여 연료의 열량 계산 '''

    # global check_conversion_unit
    fuel_to_calorie_conversion_coefficient = fuel_calorie_unit_pairs.get(
        (row[('연료사용량', '매개변수 단위')],row[('열량계수(총발열량)', '매개변수 단위')]), None)

    if fuel_to_calorie_conversion_coefficient:

        row[('연료열량_총발열량(TJ)', '')] = \
        row[('연료사용량', '매개변수값')] * row[('열량계수(총발열량)', '매개변수값')] * fuel_to_calorie_conversion_coefficient

        row[('연료열량_순발열량(TJ)', '')] = \
        row[('연료사용량', '매개변수값')] * row[('열량계수(순발열량)', '매개변수값')] * fuel_to_calorie_conversion_coefficient

    # 연료 배출량 계산이 가능하다면
    if row[('열량계수(순발열량)', '매개변수값')] != np.nan:
        
        # 배출계수 단위가 TJ 단위라면
        is_TJ_in_CO2_unit = '/TJ' in str(row[('온실가스배출계수(CO2)', '매개변수 단위')])
        is_TJ_in_CH4_unit = '/TJ' in str(row[('온실가스배출계수(CH4)', '매개변수 단위')])
        is_TJ_in_N2O_unit = '/TJ' in str(row[('온실가스배출계수(N2O)', '매개변수 단위')])
        notnull_fuel_value = not pd.isna(row[('연료사용량', '매개변수값')])
        notnull_fuel_to_calorie_conversion_coefficient = fuel_to_calorie_conversion_coefficient is not None

        if is_TJ_in_CO2_unit and is_TJ_in_CH4_unit and is_TJ_in_N2O_unit \
            and notnull_fuel_value and notnull_fuel_to_calorie_conversion_coefficient:
            lower_heating_value = \
                row[('연료사용량', '매개변수값')] * row[('열량계수(순발열량)', '매개변수값')] * fuel_to_calorie_conversion_coefficient 

            CO2_emission = lower_heating_value * row[('온실가스배출계수(CO2)', '매개변수값')] * row[('산화계수', '매개변수값')]
            CH4_emission = lower_heating_value * row[('온실가스배출계수(CH4)', '매개변수값')]
            N2O_emission = lower_heating_value * row[('온실가스배출계수(N2O)', '매개변수값')]
            
            row[('연료배출량(tCO2eq, 계산)', '')] = (CO2_emission + CH4_emission + N2O_emission)/1000
            try:
                row[('연료배출량(%)', '')] = row[('연료배출량(tCO2eq, 계산)', '')] / row[('온실가스 배출량(tCO2eq)', '')]
            except ZeroDivisionError:
                row[('연료배출량(%)', '')] = np.nan

        else:

            row[('연료배출량(tCO2eq, 계산)', '')] = '배출계수 단위, 연료 수치, 컨버젼 테이블 확인'

    else:
        check_conversion_unit[(row[('연료사용량', '매개변수 단위')],row[('열량계수(총발열량)', '매개변수 단위')])] = 'check'
        row[('연료열량_총발열량(TJ)', '')] = np.nan

    return row




def calc_calorie_emission_in_data_5(input_filename:str, output_filename:str)->None:
    ''' 1차 정리된 배출활동 자료에 대해 열량, 배출량 계산
    '''
    data = pickle_first(input_filename, Path(os.getcwd()))



    result = data.apply(lambda row:calc_heating_value_emission_for_fuel(row), axis=1)
    reordered_0_level_cols = [
        '온실가스 배출량(tCO2eq)', '활동자료 사용량', '연료열량_총발열량(TJ)', '연료열량_순발열량(TJ)', 
        '연료배출량(tCO2eq, 계산)', '연료배출량(%)',
        '연료사용량', '열량계수(총발열량)', '열량계수(순발열량)', 
        '온실가스배출계수(CO2)', '온실가스배출계수(CH4)', '온실가스배출계수(N2O)', '산화계수', 
        '외부에서공급받은열(스팀)사용량', '열(스팀)간접배출계수(CO2)', '열(스팀)간접배출계수(CH4)', 
        '열(스팀)간접배출계수(N2O)', '외부에서공급받은전력사용량', '전력간접배출계수(CO2)', 
        '전력간접배출계수(CH4)', '전력간접배출계수(N2O)', '원료투입량', '원료별배출계수(CO2)', '활동량']

    # 'reordered_0_level_cols'에 누락된 컬럼 처리
    etc_cols = list(set(result.columns.get_level_values(0)) - set(reordered_0_level_cols))
    
    reordered_cols = result.loc[:, reordered_0_level_cols+etc_cols].columns.tolist()
    result[reordered_cols].to_excel(output_filename)
    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(result[reordered_cols], output_filename_pkl, Path(os.getcwd()))


def extract_district_heat_data(input_filename:str, sheet_name:str, output_filename:str)-> None:
    '''
    집단에너지 편람 엑셀 sheet에서 집단에너지 현황 자료 추출
    '''
    data = pd.read_excel(data_path/input_filename, sheet_name = sheet_name)
    usecols = [
        '사업자코드', '분류', '관리업체명', '지정업종', '사업장 명', '설비 현황', '사용에너지', 'CHP_온실가스 배출량(tCO2eq)', 
        '열병합_기타_온실가스 배출량(tCO2eq)', 'PLB_온실가스 배출량(tCO2eq)', 'PLB_기타_온실가스 배출량(tCO2eq)', 
        'CHP_열생산량(집단)', 'PLB_열생산량(집단)', '기타_열생산량(집단)', 
        '외부수열(집단)', '자체생산_전기생산량(MWh)(집단)', '한전수전_전기생산량(MWh)(집단)']
    data = data[usecols].copy()

    data.to_excel(output_filename)
    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(data, output_filename_pkl, Path(os.getcwd()))


def load_district_heat_data(input_filename:str)->pd.DataFrame:
    ''' 집단에너지 편람 상의 데이터 중 자체 생산에 해당하는 데이터만 추출
    '''
    # 배출계수 정리 결과를 확인할 때 
    # 집단에너지사업편람의 수치가 잘못 정리되거나, 사업장 매칭이 맞지 않을 가능성을 고려해야 함

    return pd.read_excel(data_path/input_filename)
    



def calc_heating_value(row:pd.Series)->pd.Series:
    ''' 연료투입량, 총발열량을 이용하여 연료의 열량 계산 '''

    # global check_conversion_unit
    fuel_to_calorie_conversion_coefficient = fuel_calorie_unit_pairs.get(
        (row['연료사용량 단위'],row['열량계수(총발열량) 단위']), None)

    if fuel_to_calorie_conversion_coefficient:

        row['연료열량_총발열량(TJ)'] = \
        row['연료사용량 값'] * row['열량계수(총발열량) 값'] * fuel_to_calorie_conversion_coefficient

        row['연료열량_순발열량(TJ)'] = \
        row['연료사용량 값'] * row['열량계수(순발열량) 값'] * fuel_to_calorie_conversion_coefficient

    else:
        if not pd.isna(row['연료사용량 단위']) and not pd.isna(row['열량계수(총발열량) 단위']):
            print(f"연료사용량 단위 : {row['연료사용량 단위']}, 열량계수(총발열량) 단위 : {row['열량계수(총발열량) 단위']}")
        row['연료열량_총발열량(TJ)'] = np.nan
        row['연료열량_순발열량(TJ)'] = np.nan

    return row

def arrange_activities_emission_detail_in_data_5(
    input_filename:str, conditions:DataConditions, output_filename:str)->None:
    '''
    5번 배출활동 자료에서 배출량이 많은 사업장 순서대로 배출량이 많은 배출 시설순서대로 정리
    배출량은 시설단위로 합산
    
    '''
    data = pickle_first(input_filename, data_path)
    # data = data.loc[is_selected(data, conditions)].copy()
    data = data.loc[is_selected_and(data, conditions)].copy()

    district_data = load_district_heat_data('집단편람.xlsx')

    # 공백 등 컬럼 전처리
    data = arrange_parameter_col_in_data5(data)

    unstack_info = UnstackInfo(
        unstack_col = '매개변수명(비정형)',
        common_value_cols = [
            '관리업체명', '사업장 일련번호', '사업장명',
            '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '활동자료 코드명', 
            '활동자료명', '배출활동명', '활동자료 사용량', '온실가스 배출량(tCO2eq)', '시설배출량'],
        different_value_cols = [
            '매개변수명(비정형)', '매개변수 단위', '매개변수값', '매개변수적용 Tier', '정렬순서'],
        result_value_cols = [
            '활동자료 사용량', '온실가스 배출량(tCO2eq)', '시설배출량', '매개변수명(비정형)'])
    
    # index_cols = [
    #     '관리업체명', '사업장 일련번호', '사업장명',
    #     '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '활동자료 코드명', 
    #     '활동자료명', '배출활동명', '활동자료 사용량', '온실가스 배출량(tCO2eq)'
    # ]

    index_level4_cols = [
        '관리업체명', '사업장 일련번호', '사업장명',
        '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '활동자료 코드명', '활동자료명', '배출활동명', '활동자료 단위코드명']

    value_level4_cols = [
         '활동자료 사용량', '온실가스 배출량(tCO2eq)', '연료열량_총발열량(TJ)', '연료열량_순발열량(TJ)', '연료사용량', '열량계수(총발열량)', '열량계수(순발열량)', '원료투입량', '원료별배출계수(CO2)']

    index_level3_cols = [
        '관리업체명', '사업장 일련번호', '사업장명',
        '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '활동자료 코드명', 
        '활동자료명', '배출활동명']
    value3_cols = ['온실가스 배출량(tCO2eq)']
    
    index_level2_cols = [
        '관리업체명', '사업장 일련번호', '사업장명', '배출시설 일련번호', '배출시설 코드명', '자체 시설명']
    value2_cols = ['시설배출량']

    index_level1_cols = [
        '관리업체명', '사업장 일련번호', '사업장명']
    value1_cols = ['사업장배출량']

    # data_grp3 = data.groupby(index_level3_cols, as_index=False)[value3_cols].sum().sort_values(
    #     by=value3_cols[0], ascending=False)

    data_grp2 = data[index_level2_cols+value2_cols].drop_duplicates().sort_values(by=value2_cols[0], ascending=False)
    data_grp1 = data[index_level1_cols+value1_cols].drop_duplicates().sort_values(by=value1_cols[0], ascending=False)

    workplace_col = '사업장 일련번호'
    equipment_col = '배출시설 일련번호'

    usecols2 = ['배출시설 일련번호', '배출시설 코드명', '자체 시설명'] + value2_cols
    # usecols3 = ['배출시설 일련번호', '활동자료 코드명', '활동자료명', '배출활동명', '온실가스 배출량(tCO2eq)'] 
    usecols4 = [
        '배출시설 일련번호', '활동자료 코드명', '활동자료명', '배출활동명', '온실가스 배출량(tCO2eq)',
        '연료열량_총발열량(TJ)', '연료열량_순발열량(TJ)', '연료사용량 값', '연료사용량 단위', '열량계수(총발열량) 값', '열량계수(총발열량) 단위', 
        '열량계수(순발열량) 값', '열량계수(순발열량) 단위', '원료투입량 값', '원료투입량 단위', '원료별배출계수(CO2) 값', '원료별배출계수(CO2) 단위']

    result = []
    # for workplace in tqdm(data_grp1[workplace_col].unique().tolist()[:10]):
    for workplace in tqdm(data_grp1[workplace_col].unique().tolist()):
        is_workplace1 = data_grp1[workplace_col] == workplace
        is_workplace2 = data_grp2[workplace_col] == workplace
        # is_workplace3 = data_grp3[workplace_col] == workplace

        sub_result = []
        for equipment in data_grp2.loc[is_workplace2, equipment_col].unique().tolist():
            is_equipment2 = data_grp2[equipment_col] == equipment
            # is_equipment3 = data_grp3[equipment_col] == equipment


            is_workplace4 = data[workplace_col] == workplace
            is_equipment4 = data[equipment_col] == equipment
            data_to_unstack = data.loc[is_workplace4 & is_equipment4].copy()
            coefficient_detail = unstack_cols_in_data5(data_to_unstack, unstack_info).reset_index()    

            sub_result.append(
                pd.concat(
                    [
                        data_grp2.loc[is_workplace2 & is_equipment2].reset_index(drop=True)[usecols2].assign(blank = np.nan), 
                        # data_grp3.loc[is_workplace3 & is_equipment3].reset_index(drop=True)[usecols3].assign(blank = np.nan),
                        coefficient_detail.reset_index(drop=True)[usecols4]
                    ], axis=1
                )
            )
        sub_result = pd.concat(sub_result, axis=0)

        # 배출활동명이 '기체연료연소', '액체연료연소', '고체연료연소' 인 배출활동에 대해 배출시설별로 sub_result의 결과 정리
        # 
        # sub_result.to_excel('sub_result.xlsx')
        acitivities_to_summary = ['기체연료연소', '액체연료연소', '고체연료연소']
        is_activity_to_summary = check_isin(series = sub_result['배출활동명'], values = acitivities_to_summary)
        

        # 해당 자료가 있다면
        # 발전용내연, 화력발전은 열병합으로 통합
        columns = [

            # 배출계수 계산
            '전체_열병합_온실가스 배출량(tCO2eq)', 
            '열병합_발전량(TJ)', 
            '연료_총발열량(TJ)', 
            '연료_순발열량(TJ)', 
            '열병합_발전효율(%)', 
            '열병합_열생산량(TJ)', 
            '열병합_열생산효율(%)', 
            '열병합_종합효율(%)', 
            '보일러_온실가스 배출량(tCO2eq)', 
            '보일러_열생산량(TJ)', 
            '보일러_열생산효율(%)', 
            '전력배출계수(tCO2eq/TJ)', 
            '전력배출계수(tCO2eq/MWh)', 
            '종합배출계수(tCO2eq/TJ)', 
            '종합배출계수(tCO2eq/MWh)',

            # '명세서_항목',
            '열병합_온실가스 배출량(tCO2eq)',
            '열병합_연료_총발열량(TJ)',
            '열병합_연료_순발열량(TJ)',
            '발전용내연_온실가스 배출량(tCO2eq)',
            '발전용내연_연료_총발열량(TJ)',
            '발전용내연_연료_순발열량(TJ)',
            '화력발전_온실가스 배출량(tCO2eq)',
            '화력발전_연료_총발열량(TJ)',
            '화력발전_연료_순발열량(TJ)',
            '보일러_온실가스 배출량(tCO2eq)',
            '보일러_연료_총발열량(TJ)',
            '보일러_연료_순발열량(TJ)',

            # '집단에너지사업편람_항목',
            'CHP_열생산량(Gcal)',
            'PLB_열생산량(Gcal)',
            '기타_열생산량(Gcal)',
            '외부수열(Gcal)',
            '자체발전량(MWh)',
            '한전수전량(MWh)']

        form_frame = pd.DataFrame(index=[0], columns = columns)
        # coefficient_result = []
        if is_activity_to_summary.sum() > 0:
            frame_to_summary = sub_result.loc[is_activity_to_summary].copy()
            equipments = frame_to_summary['배출시설 코드명'].dropna().unique().tolist()

            # 편람 데이터 입력
            is_workplace_district = district_data['사업장 일련번호'] == workplace
            form_frame['PLB_열생산량(Gcal)'] = district_data.loc[is_workplace_district, 'PLB_열생산량(Gcal)'].sum()
            form_frame['CHP_열생산량(Gcal)'] = district_data.loc[is_workplace_district, 'CHP_열생산량(Gcal)'].sum()
            form_frame['자체발전량(MWh)'] = district_data.loc[is_workplace_district, '자체발전량(MWh)'].sum()
            form_frame['기타_열생산량(Gcal)'] = district_data.loc[is_workplace_district, '기타_열생산량(Gcal)'].sum()
            form_frame['외부수열(Gcal)'] = district_data.loc[is_workplace_district, '외부수열(Gcal)'].sum()
            form_frame['한전수전량(MWh)'] = district_data.loc[is_workplace_district, '한전수전량(MWh)'].sum()

            for sub_equipment in equipments:
                # equipment = '일반 보일러시설'
                frame_to_summary['배출시설 코드명'] = frame_to_summary['배출시설 코드명'].ffill()
                is_equipment = frame_to_summary['배출시설 코드명'] == sub_equipment
                emission = frame_to_summary.loc[is_equipment, '온실가스 배출량(tCO2eq)'].sum()
                HHV_TJ = frame_to_summary.loc[is_equipment, '연료열량_총발열량(TJ)'].sum()
                LHV_TJ = frame_to_summary.loc[is_equipment, '연료열량_순발열량(TJ)'].sum()

                if sub_equipment == '일반 보일러시설':

                    form_frame['보일러_온실가스 배출량(tCO2eq)'] = emission
                    form_frame['보일러_연료_총발열량(TJ)'] = HHV_TJ
                    form_frame['보일러_연료_순발열량(TJ)'] = LHV_TJ

                elif sub_equipment == '발전용 내연기관':

                    form_frame['발전용내연_온실가스 배출량(tCO2eq)'] = emission
                    form_frame['발전용내연_연료_총발열량(TJ)'] = HHV_TJ
                    form_frame['발전용내연_연료_순발열량(TJ)'] = LHV_TJ

                elif sub_equipment == '화력 발전시설':

                    form_frame['화력발전_온실가스 배출량(tCO2eq)'] = emission
                    form_frame['화력발전_연료_총발열량(TJ)'] = HHV_TJ
                    form_frame['화력발전_연료_순발열량(TJ)'] = LHV_TJ


                elif sub_equipment == '열병합 발전시설':

                    form_frame['열병합_온실가스 배출량(tCO2eq)'] = emission
                    form_frame['열병합_연료_총발열량(TJ)'] = HHV_TJ
                    form_frame['열병합_연료_순발열량(TJ)'] = LHV_TJ


                # # 발전용 내연기관 등
                # # 수식이 맞는지 계산 결과 확인 필요
                # elif sub_equipment == '발전용 내연기관' or sub_equipment == '화력 발전시설':
                #     heat_chp_Gcal = 0
                #     heat_chp_TJ = heat_chp_Gcal * 4.1868/1000
                #     electricity_chp_MWh = 0
                #     electricity_chp_TJ = electricity_chp_MWh * 3.6/1000
                #     generation_efficiency = electricity_chp_TJ / HHV_TJ
                #     therm_efficiency = heat_chp_TJ / HHV_TJ

                #     if HHV_TJ == 0:
                #         generation_efficiency = np.nan
                #         therm_efficiency = np.nan

                #     else:
                #         generation_efficiency = electricity_chp_TJ / HHV_TJ
                #         therm_efficiency = heat_chp_TJ / HHV_TJ

                # if is_boiler:
                #     total_effciency = heat_plb_TJ / HHV_TJ
                #     total_emission_coeff_TJ = emission / (heat_plb_TJ) 
                #     total_emission_coeff_MWh = emission / (heat_plb_TJ /3.6 * 1000) 
                #     heat_TJ = heat_plb_TJ
                #     heat_chp_TJ = heat_plb_TJ
                #     electricity_TJ = 0
                #     electricity_chp_TJ = 0
                #     electricity_chp_MWh = 0
                #     generation_efficiency = 0
                #     electric_emission_coeff_TJ = 0
                #     electric_emission_coeff_MWh = 0

                # else:
                    
                #     electric_emission_coeff_TJ = emission / electricity_chp_TJ
                #     electric_emission_coeff_MWh = emission / electricity_chp_MWh
                #     heat_TJ = heat_chp_TJ

                # # dataframe 생성

                # total_effciency = (heat_chp_TJ + electricity_chp_TJ) / HHV_TJ
                # total_emission_coeff_TJ = emission / (electricity_chp_TJ + heat_chp_TJ) 
                # total_emission_coeff_MWh = emission / (electricity_chp_MWh + heat_chp_TJ /3.6 * 1000) 

                # summary = pd.DataFrame(
                #     {'사업장 일련번호':workplace, 
                #     '배출시설 코드명':sub_equipment, 
                #     '온실가스 배출량(tCO2eq)':emission, 
                #     '총발열량(TJ)':HHV_TJ,
                #     '순발열량(TJ)':LHV_TJ,
                #     '열생산량(TJ)': heat_TJ,
                #     '발전량(TJ)':electricity_chp_TJ,
                #     '발전효율(%)':generation_efficiency,
                #     '열생산효율(%)':therm_efficiency,
                #     '종합효율(%)':total_effciency,
                #     '전력배출계수(tCO2eq/TJ)':electric_emission_coeff_TJ,
                #     '전력배출계수(tCO2eq/MWh)':electric_emission_coeff_MWh,
                #     '종합배출계수(tCO2eq/TJ)':total_emission_coeff_TJ,
                #     '종합배출계수(tCO2eq/MWh)':total_emission_coeff_MWh}, index=[0])

            # coefficient_result_concat = pd.concat(coefficient_result, axis=0)

        # else:
        #     coefficient_result_concat = pd.DataFrame(
        #         {'사업장 일련번호':workplace, 
        #         '배출시설 코드명':np.nan, 
        #         '온실가스 배출량(tCO2eq)':np.nan, 
        #         '총발열량(TJ)':np.nan,
        #         '순발열량(TJ)':np.nan,
        #         '열생산량(TJ)': np.nan,
        #         '발전량(TJ)':np.nan,
        #         '발전효율(%)':np.nan,
        #         '열생산효율(%)':np.nan,
        #         '종합효율(%)':np.nan,
        #         '전력배출계수(tCO2eq/TJ)':np.nan,
        #         '전력배출계수(tCO2eq/MWh)':np.nan,
        #         '종합배출계수(tCO2eq/TJ)':np.nan,
        #         '종합배출계수(tCO2eq/MWh)':np.nan}, index=[0])

        result.append(
            pd.concat(
                [
                    data_grp1.loc[is_workplace1].reset_index(drop=True).assign(blank = np.nan),
                    form_frame.reset_index(drop=True).assign(blank = np.nan),
                    sub_result.reset_index(drop=True)
                ], axis=1))

    # 조건에 만족하는 데이터가 없는 경우
    if len(result) == 0:
        raise ValueError('조건(고정연소배출활동 등)에 만족하는 데이터가 존재하지 않음')

    result = pd.concat(result, axis=0)
    result.to_excel(output_filename)
    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(result, output_filename_pkl, Path(os.getcwd()))


def arrange_activities_emission_coefficient_in_data_5(
    input_filename:str, conditions:DataConditions, output_filename:str)->None:
    '''
    5번 배출활동 자료에서 배출량이 많은 사업장 순서대로 배출량이 많은 배출 시설순서대로 정리
    배출량은 시설단위로 합산
    배출계수를 어떻게 적용하는지 확인
    
    '''
    data = pickle_first(input_filename, data_path)
    # data = data.loc[is_selected(data, conditions)].copy()
    data = data.loc[is_selected_and(data, conditions)].copy()

    # 공백 등 컬럼 전처리
    data = arrange_parameter_col_in_data5(data)

    unstack_info = UnstackInfo(
        unstack_col = '매개변수명(비정형)',
        
        # unstack 대상 정보와 무관한 공통 항목
        common_value_cols = [
            '관리업체명', '사업장 일련번호', '사업장명',
            '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '활동자료 코드명', 
            '활동자료명', '배출활동명', '활동자료 사용량', '온실가스 배출량(tCO2eq)', '시설배출량'],
        
        # unstack 대상 정보, 행별로 값이 다름
        different_value_cols = [
            '매개변수명(비정형)', '매개변수 단위', '매개변수값', '매개변수적용 Tier', '정렬순서'],
        
        # 정리결과에서 계산이 필요한 컬럼
        result_value_cols = [
            '활동자료 사용량', '온실가스 배출량(tCO2eq)', '시설배출량', '매개변수명(비정형)'],
            
        # unstack 결과에서 사용할 컬럼을 선택
        # stacked 정보는 배출활동명에 따라 다른 정보를 가짐
        # 배출활동명이 액체, 기체, 고체연료연소에 대한 unstack 결과(multiindex column)
        # unstacked 정보(multiindex) 중에서 필요한 컬럼을 선별한 후에 이름을 변경

        unstacked_fuel_combustion_selected_cols = [
            ('온실가스 배출량(tCO2eq)', ''), 
            ('연료사용량', '매개변수값'),
            ('연료사용량', '매개변수 단위'), 
            ('열량계수(총발열량)', '매개변수값'),
            ('열량계수(총발열량)', '매개변수 단위'),
            ('열량계수(순발열량)', '매개변수값'),
            ('열량계수(순발열량)', '매개변수 단위'),
            ('온실가스배출계수(CO2)', '매개변수 단위'),
            ('온실가스배출계수(CO2)', '매개변수값'),
            ('온실가스배출계수(CH4)', '매개변수 단위'),
            ('온실가스배출계수(CH4)', '매개변수값'),
            ('온실가스배출계수(N2O)', '매개변수 단위'),
            ('온실가스배출계수(N2O)', '매개변수값'),
            ('산화계수', '매개변수값')],

        unstacked_fuel_combustion_selected_rename_cols = [
            '온실가스 배출량(tCO2eq)',
            '연료사용량 값',
            '연료사용량 단위', 
            '열량계수(총발열량) 값',
            '열량계수(총발열량) 단위',
            '열량계수(순발열량) 값',
            '열량계수(순발열량) 단위',
            '온실가스배출계수(CO2) 단위',
            '온실가스배출계수(CO2) 값', 
            '온실가스배출계수(CH4) 단위',
            '온실가스배출계수(CH4) 값',
            '온실가스배출계수(N2O) 단위',
            '온실가스배출계수(N2O) 값', 
            '산화계수 값'],

        # 배출활동명이 연료전지에 대한 unstack 결과(multiindex column)
        unstacked_fuelcell_selected_cols = [
            ('온실가스 배출량(tCO2eq)', ''), 
            ('원료투입량', '매개변수값'),
            ('원료투입량', '매개변수 단위'), 
            ('원료별배출계수(CO2)', '매개변수값'),
            ('원료별배출계수(CO2)', '매개변수 단위')],

        unstacked_fuelcell_selected_rename_cols = [             
            '온실가스 배출량(tCO2eq)', 
            '원료투입량 값',
            '원료투입량 단위',
            '원료별배출계수(CO2) 값',
            '원료별배출계수(CO2) 단위'],

        # unstack 결과 정리 파일의 컬럼 포멧
        # unstacked 후 rename된 컬럼을 대상으로 필요한 컬럼을 순서대로 기재
        output_file_cols = [
            '온실가스 배출량(tCO2eq)',
            '연료사용량 값',
            '연료사용량 단위', 
            '열량계수(총발열량) 값',
            '열량계수(총발열량) 단위',
            '열량계수(순발열량) 값',
            '열량계수(순발열량) 단위',
            '온실가스배출계수(CO2) 단위',
            '온실가스배출계수(CO2) 값', 
            '온실가스배출계수(CH4) 단위',
            '온실가스배출계수(CH4) 값',
            '온실가스배출계수(N2O) 단위',
            '온실가스배출계수(N2O) 값', 
            '산화계수 값'])
    
    # index_cols = [
    #     '관리업체명', '사업장 일련번호', '사업장명',
    #     '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '활동자료 코드명', 
    #     '활동자료명', '배출활동명', '활동자료 사용량', '온실가스 배출량(tCO2eq)'
    # ]

    index_level4_cols = [
        '관리업체명', '사업장 일련번호', '사업장명',
        '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '활동자료 코드명', '활동자료명', '배출활동명', '활동자료 단위코드명']

    value_level4_cols = [
         '활동자료 사용량', '온실가스 배출량(tCO2eq)', '연료열량_총발열량(TJ)', '연료열량_순발열량(TJ)', '연료사용량', '열량계수(총발열량)', '열량계수(순발열량)', '원료투입량', '원료별배출계수(CO2)']

    index_level3_cols = [
        '관리업체명', '사업장 일련번호', '사업장명',
        '배출시설 일련번호', '배출시설 코드명', '자체 시설명', '활동자료 코드명', 
        '활동자료명', '배출활동명']
    value3_cols = ['온실가스 배출량(tCO2eq)']
    
    index_level2_cols = [
        '관리업체명', '사업장 일련번호', '사업장명', '배출시설 일련번호', '배출시설 코드명', '자체 시설명']
    value2_cols = ['시설배출량']

    index_level1_cols = [
        '관리업체명', '사업장 일련번호', '사업장명']
    value1_cols = ['사업장배출량']

    # data_grp3 = data.groupby(index_level3_cols, as_index=False)[value3_cols].sum().sort_values(
    #     by=value3_cols[0], ascending=False)

    data_grp2 = data[index_level2_cols+value2_cols].drop_duplicates().sort_values(by=value2_cols[0], ascending=False)
    data_grp1 = data[index_level1_cols+value1_cols].drop_duplicates().sort_values(by=value1_cols[0], ascending=False)

    workplace_col = '사업장 일련번호'
    equipment_col = '배출시설 일련번호'

    usecols2 = ['배출시설 일련번호', '배출시설 코드명', '자체 시설명'] + value2_cols
    # usecols3 = ['배출시설 일련번호', '활동자료 코드명', '활동자료명', '배출활동명', '온실가스 배출량(tCO2eq)'] 
    # usecols4 = unstack_info.output_file_cols


    result = []
    # for workplace in tqdm(data_grp1[workplace_col].unique().tolist()[:10]):
    for workplace in tqdm(data_grp1[workplace_col].unique().tolist()):
        is_workplace1 = data_grp1[workplace_col] == workplace
        is_workplace2 = data_grp2[workplace_col] == workplace
        # is_workplace3 = data_grp3[workplace_col] == workplace

        sub_result = []
        for equipment in data_grp2.loc[is_workplace2, equipment_col].unique().tolist():
            is_equipment2 = data_grp2[equipment_col] == equipment
            # is_equipment3 = data_grp3[equipment_col] == equipment

            is_workplace4 = data[workplace_col] == workplace
            is_equipment4 = data[equipment_col] == equipment
            data_to_unstack = data.loc[is_workplace4 & is_equipment4].copy()
            coefficient_detail = unstack_cols_in_data5(
                data = data_to_unstack, 
                unstack_info = unstack_info, 
                activity_name_keyword = '연료연소').reset_index()    

            sub_result.append(
                pd.concat(
                    [
                        data_grp2.loc[is_workplace2 & is_equipment2].reset_index(drop=True)[usecols2].assign(blank = np.nan), 
                        # data_grp3.loc[is_workplace3 & is_equipment3].reset_index(drop=True)[usecols3].assign(blank = np.nan),
                        # coefficient_detail.reset_index(drop=True)[usecols4]
                        coefficient_detail.reset_index(drop=True)
                    ], axis=1
                )
            )
        sub_result = pd.concat(sub_result, axis=0)

        # 배출활동명이 '기체연료연소', '액체연료연소', '고체연료연소' 인 배출활동에 대해 배출시설별로 sub_result의 결과 정리
        # 
        # sub_result.to_excel('sub_result.xlsx')
        # acitivities_to_summary = ['기체연료연소', '액체연료연소', '고체연료연소']
        # is_activity_to_summary = check_isin(series = sub_result['배출활동명'], values = acitivities_to_summary)

        result.append(
            pd.concat(
                [
                    data_grp1.loc[is_workplace1].reset_index(drop=True).assign(blank = np.nan),
                    sub_result.reset_index(drop=True)
                ], axis=1))

    # 조건에 만족하는 데이터가 없는 경우
    if len(result) == 0:
        raise ValueError('조건(고정연소배출활동 등)에 만족하는 데이터가 존재하지 않음')

    result = pd.concat(result, axis=0)
    result.to_excel(output_filename)
    output_filename_pkl = output_filename[:output_filename.rfind('.')]+'.pkl'
    save_pickle(result, output_filename_pkl, Path(os.getcwd()))

def find_enterprises_using_workplace_no(
    frame:pd.DataFrame, workplace_no_list:List[str]=None)->pd.Series:
    ''' 사업장 번호가 저장된 목록을 이용하여 해당 사업장이 소속된 법인과 해당 법인에 소속된 사업장 목록을 반환 '''

    if workplace_no_list is None or frame is None:
        print('check frame or workplace_no_list if empty')
        return

    workplace_no_col = '사업장 일련번호'
    is_selected_workplaces = check_isin(series=frame[workplace_no_col], values=workplace_no_list)

    enterprise_no_col ='관리업체 일련번호'
    enterprise_list = frame.loc[is_selected_workplaces, enterprise_no_col].drop_duplicated().tolist()

    is_selected_enterprise = check_isin(series=frame[enterprise_no_col], values=enterprise_list)
    workplace_list_after = frame.loc[is_selected_enterprise, workplace_no_col].drop_duplicated().tolist()

    return enterprise_list, workplace_list_after


def extract_enterprise_numbers_from_workplace_numbers_in_frame(
    frame:pd.DataFrame, fi:FrameInfo, workplace_numbers:List[str])->List[str]:
    ''' 사업장 일련번호를 이용하여 사업장이 속한 법인의 법인 일련번호 추출 '''

    is_selected_workplace = check_isin(frame[fi.workplace_no_col], values=workplace_numbers)
    return frame.loc[is_selected_workplace, fi.enterprise_no_col].unique().tolist()

def extract_workplace_numbers_from_enterprise_numbers_in_frame(
    frame:pd.DataFrame, fi:FrameInfo, enterprise_numbers:List[str])->List[str]:
    ''' 법인 일련번호를 이용하여 법인에 소속된 사업장의 사업장 일련번호 추출 '''

    is_selected_enterprise = check_isin(frame[fi.enterprise_no_col], values=enterprise_numbers)
    return frame.loc[is_selected_enterprise, fi.workplace_no_col].unique().tolist()

def expand_workplace_numbers_by_enterprise_numbers_in_data(
    filename:str, fi:FrameInfo, workplace_numbers:List[str])->List[str]:
    '''
    입력된 사업장 일련번호에 대해 해당 사업장이 관리업체의 일련번호를 확인한 후에 해당 관리업체에 소속된 사업장 일련번호를 반환
    '''
    frame = pickle_first(filename, data_path)
    fi = FrameInfo()
    enterprise_numbers = extract_enterprise_numbers_from_workplace_numbers_in_frame(frame, fi, workplace_numbers)
    workplace_numbers = extract_workplace_numbers_from_enterprise_numbers_in_frame(frame, fi, enterprise_numbers)
    return workplace_numbers


def load_workplace_number_in_data(
    filename:str, sheet_name:str, workplace_number_col:str, output_filename_pkl:str)->List[str]:
    ''' 파일에 저장된 사업장 일련번호 로딩 '''
    
    data = pd.read_excel(data_path/filename, sheet_name=sheet_name)
    workplace_numbers = data[workplace_number_col].tolist()
    save_pickle(workplace_numbers, output_filename_pkl, os.getcwd())
    return workplace_numbers

if __name__ == '__main__':

    # # 데이터 통합
    # data6_total = merge_data()
    # pre_review_result = load_pre_review_result()

    # # 기존 결과 확인
    # data6_total = data6_total.join(pre_review_result)
    # data6_total.to_excel('data6_total.xlsx')

    # 전체 명세서 제출 기업 발전량 확인
    # data = arrange_intensity_data_2_all_electricity()

    # 발전 관련 업종 확인
    # generation_related_biz_2 = arrange_intensity_data_2_all_electricity()

    # 발전 관련 시설
    # data = arrange_all_generation_equipment_data_3()

    # 배출량 합산
    # emission = sum_energy_generation_emission_in_data_5()
    # generation_related_biz = arrange_generation_related_biz_in_data_6()
    
    # 발전량 확인
    # main_product = extract_workplace_main_product()
    
    # 공정 확인
    # main_process = extrace_workplace_process_information()

    # 연료전지 분리
    # seperate_fuelcell_in_process_info_in_data_6()

    # 배출활동(5번) 자료 정리
    # select_energy_generation_emission_equipment_in_data_5(
    #     input_filename = '05.배출활동별 배출량_전체업체_2020_2021101400.csv',
    #     output_filename = '5_1 배출활동 정리자료_업종제한해제.xlsx', 
    #     sectors = None)

    # sum_emission_by_activity_in_data_5(
    #     input_filename = '5_1 배출활동 정리자료_업종제한해제.xlsx',
    #     output_filename = '5_2 배출활동 정리자료_업종제한해제.xlsx')

    # # 공정배출(6번) 자료 정리
    # select_energy_generation_equipment_data_6(
    #     input_filename = '06.공정별 원단위_전체업체_2021101400.csv',
    #     output_filename = '6_1 공정 정리자료_업종제한해제.xlsx', 
    #     sectors = None)

    # process_production_amount_data_6(
    #     input_filename = '6_1 공정 정리자료_업종제한해제.xlsx', 
    #     output_filename = '6_2 생산량 정리자료_업종제한해제.xlsx')

    # eliminate_duplicated_production_amount_data_6(
    #     input_filename = '6_2 생산량 정리자료_업종제한해제.xlsx', 
    #     output_filename = '6_3 생산량 정리자료_업종제한해제.xlsx')

    # convert_production_unit_data_6(
    #     input_filename = '6_3 생산량 정리자료_업종제한해제.xlsx', 
    #     output_filename = '6_4 생산량 정리자료_업종제한해제.xlsx')

        
    # # 배출량 비교
    # # 배출활동(5번) 파일은 배출활동별로 배출량 합계가 존재하고, 각 배출량 합계가 사업장의 배출량과 일치하므로 
    # # 가장 배출량이 세부적으로 기재되어 있는 것이 장점. 
    # # 그러나 배출계수 적용 오류로 배출량이 정확하게 계산되지 않는 경우가 존재하므로 검증 필요
    # # 공정(6번) 파일은 공정단위로 데이터를 기재하는 과정에서 일부 시설의 배출량이 누락되거나, 중복으로 기재될 수 있음
    # # 6번 파일 상의 배출원단위, 에너지원단위는 100% 신뢰할 수 없음(참고용으로만 사용 가능)
    # # 5번, 6번 파일의 정리 결과를 비교하여 수치가 동일하지 않은 경우 누락 여부의 검토 필요

    # 파일 통합
    # compare_emission_activity_process_data_56(
    #     input_filenames = ['5_2 배출활동 정리자료_업종제한해제.xlsx','6_4 생산량 정리자료_업종제한해제.xlsx'],
    #     output_filename = '56_1 자료비교 결과_업종제한해제.xlsx')

    # 중복 제거, 2개의 다른 결과를 비교하여 공통되는 것을 삭제할 필요가 있을 경우만 사용
    # delete_same_equipment_data_56(
    #     input_filenames = ['56_1 자료비교 결과_생산량변환.xlsx','56_1 자료비교 결과_업종제한해제.xlsx'],
    #     output_filename = '56_1 추가결과_업종제한해제.xlsx')

    # 배출활동 데이터 재정리
    # 투입 연료의 열량을 계산해서 생산된 열과 전력량 사이에 중복이 있는지 판단
    # arrange_activities_in_data_5(
    #     input_filename = '05.배출활동별 배출량_전체업체_2020_2021101400.csv',
    #     output_filename = '5_1 배출활동 정리자료_계수정리.xlsx')

    # calc_calorie_emission_in_data_5(
    #     input_filename = '5_1 배출활동 정리자료_계수정리.xlsx',
    #     output_filename = '5_2 배출활동 정리자료_열량계산.xlsx')

    # merge_activity_process_data_56(
    #     input_filenames = ['5_2 배출활동 정리자료_열량계산.xlsx','6_4 생산량 정리자료.xlsx'],
    #     output_filename = '56_1 자료비교 결과_열량.xlsx')

    # 집단에너지 사업자 정리
    # extract_district_heat_data(
    #     input_filename = '(열병합발전) 온실가스 배출계수_백데이터.xlsx',
    #     sheet_name = '백데이터',
    #     output_filename = '집단에너지사업자 생산현황.xlsx')

    # to do : 사업자 정보가 정리된 후에 구현
    # load_district_heat_data(
    #     input_filename = '집단에너지사업자 생산현황.xlsx',
    #     output_filename = 'd_1 집단에너지 현황.xlsx')


    # 집단에너지 사업장 정리
    # 5번 배출활동 자료에서 배출량이 많은 사업장 순서대로 배출량이 많은 배출 시설순서대로 정리
    # 배출량은 시설단위로 합산
    # arrange_main_activity_item_in_data_5(
    #     input_filename = '05.배출활동별 배출량_전체업체_2020_2021101400.csv',
    #     output_filename = '5_1 배출활동 주요항목정리2.xlsx')    


    # 분석 대상 사업장 번호 로딩
    # workplace_numbers = load_workplace_number_in_data(
    #     filename = '검토결과_220204.xlsx',
    #     sheet_name = 'Sheet1',
    #     workplace_number_col= '번호',
    #     output_filename_pkl = 'workplace_numbers_220204.pkl')

    # # 사업장 번호 확장
    # fi = FrameInfo()
    # workplace_numbers_ex = expand_workplace_numbers_by_enterprise_numbers_in_data(
    #     filename = '05.배출활동별 배출량_전체업체_2020_2021101400.csv',
    #     fi = fi, workplace_numbers = workplace_numbers)

    # 임시로 특정 사업장 지정
    # workplace_numbers_ex = ['I1700100030440']



    # # 배출활동 배출량 정리
    # conditions = DataConditions(
    #     year=2020, 
    #     # equipment_divs = [
    #     # '개질공정', '발전용 내연기관', '연료전지', '열병합 발전시설', '일반 보일러시설', '화력 발전시설', '기타'],
    #     equipment_divs = [
    #     '개질공정', '연료전지', '기타'],

    #     # activity_divs = ['고정연소'],
    #     # workplace_numbers = workplace_numbers
    #     )

    # arrange_activities_emission_detail_in_data_5(
    #     input_filename = '05.배출활동별 배출량_전체업체_2020_2021101400.csv',
    #     conditions = conditions,
    #     output_filename = '5_1 배출활동별 배출량 합산_220207_연료전지.xlsx')


    # 유연탄 발전소 배출시설 일련번호
    coal_fired_equipments = pd.read_csv('유연탄 발전소 배출시설 일련번호.csv', encoding='cp949')
    coal_fired_equipments = coal_fired_equipments['유연탄 발전소 배출시설 일련번호'].str.replace(' ', '').tolist()

    # 배출활동 배출량 정리
    conditions = DataConditions(
        year=2020, 
        equipment_numbers = coal_fired_equipments
        )

    arrange_activities_emission_coefficient_in_data_5(
        input_filename = '05.배출활동별 배출량_전체업체_2020_2021101400.csv',
        conditions = conditions,
        output_filename = '5_1 배출활동별 배출량 합산_220209_유연탄.xlsx')




